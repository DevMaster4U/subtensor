//! Front-of-pool injection.
//!
//! Subtensor assigns flat priority `1` to normal EVM transactions, so the
//! ready queue is ordered first-come-first-served within that tier.
//!
//! **Submit** happens synchronously on the next block announce via
//! [`crate::sync_inject`] (same hook as announce mode). This task only watches
//! block imports to detect inclusion and advance the nonce — it does not wait
//! for import before the first inject.

use crate::control::BotControl;
use crate::inject_shared::{InjectResult, PendingTx, SharedInjectState, build_tx_at, inject_sync};
use crate::transact::{TxConfig, TxPropagator, fetch_nonce};
use fp_rpc::EthereumRuntimeRPCApi;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use node_subtensor_runtime::opaque::Block;
use sc_client_api::BlockchainEvents;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_runtime::traits::Block as BlockT;
use std::sync::Arc;

/// Spawn the pool-front injector background task.
pub fn start_pool_injector<C, P>(
    task_manager: &sc_service::TaskManager,
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    propagator: TxPropagator,
) where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + BlockchainEvents<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >
        + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-pool-injector",
        None,
        run(client, pool, control, state, propagator),
    );
}

async fn wait_for_pending<C>(client: &C, cfg: &TxConfig, state: &SharedInjectState)
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    loop {
        match fetch_nonce(client, cfg.from) {
            Ok(nonce) => {
                state.init_pending(build_tx_at(cfg, nonce));
                return;
            }
            Err(e) => {
                log::debug!(
                    target: "bot::pool_inject",
                    "waiting for runtime before prebuild: {e}",
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn process_import<C, P>(
    client: &C,
    pool: &P,
    propagator: &TxPropagator,
    control: &BotControl,
    cfg: &TxConfig,
    state: &SharedInjectState,
) where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >,
{
    if !control.inject_mode().uses_pool_front() || !control.is_running() {
        state.clear_inject_paused();
        return;
    }

    if state.inject_paused() {
        return;
    }

    let chain_nonce = match fetch_nonce(client, cfg.from) {
        Ok(n) => n,
        Err(e) => {
            log::debug!(
                target: "bot::pool_inject",
                "nonce fetch failed: {e}",
            );
            return;
        }
    };

    let Some(pending) = state.pending() else {
        return;
    };

    if chain_nonce > pending.nonce {
        control.on_sent();
        log::info!(
            target: "bot::pool_inject",
            "✅ tx included, nonce={} (remaining={:?})",
            pending.nonce,
            control.tx_remaining(),
        );
        let advanced = state.advance_pending(cfg, chain_nonce);
        log::info!(
            target: "bot::pool_inject",
            "✅ tx pre-built for next inject, nonce={}",
            advanced.nonce,
        );
    }

    // Inject (or refresh) on every block import so we never skip a block when an
    // early announce was rejected by the best+1 filter or the announce inject ran
    // one block too early.
    if control.should_send() {
        if let Some(p) = state.pending() {
            ensure_in_pool(client, pool, propagator, state, &p);
        }
    }
}

fn ensure_in_pool<C, P>(
    client: &C,
    pool: &P,
    propagator: &TxPropagator,
    state: &SharedInjectState,
    pending: &PendingTx,
) where
    C: sp_blockchain::HeaderBackend<Block>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >,
{
    let at_hash = client.info().best_hash;
    log::info!(
        target: "bot::pool_inject",
        "📌 import inject (nonce={}, at={:?})",
        pending.nonce,
        at_hash,
    );
    match inject_sync(pool, at_hash, pending, propagator) {
        InjectResult::Queued => {
            state.mark_queued(pending.nonce);
        }
        InjectResult::Fatal => state.set_inject_paused(true),
        InjectResult::Retry => {}
    }
}

fn run<C, P>(
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    propagator: TxPropagator,
) -> BoxFuture<'static, ()>
where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + BlockchainEvents<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >
        + 'static,
{
    async move {
        let cfg = TxConfig::from_env();
        wait_for_pending(client.as_ref(), &cfg, state.as_ref()).await;

        if let Some(pending) = state.pending() {
            log::info!(
                target: "bot::pool_inject",
                "✅ initial tx pre-built, nonce={} (pool-front idle — call bot_startTxsFront or bot_startTxsFast)",
                pending.nonce
            );
        }

        let mut imports = client.import_notification_stream().fuse();

        loop {
            tokio::select! {
                maybe_import = imports.next() => {
                    if maybe_import.is_none() {
                        log::warn!(target: "bot::pool_inject", "import stream ended");
                        break;
                    }
                }
                () = control.pool_wake() => {
                    log::debug!(
                        target: "bot::pool_inject",
                        "armed — inject on next block announce (sync hook)",
                    );
                }
            }

            process_import(
                client.as_ref(),
                pool.as_ref(),
                &propagator,
                &control,
                &cfg,
                state.as_ref(),
            )
            .await;
        }
    }
    .boxed()
}
