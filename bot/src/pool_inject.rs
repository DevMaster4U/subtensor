//! Front-of-pool injection.
//!
//! Subtensor assigns flat priority `1` to normal EVM transactions, so the
//! ready queue is ordered first-come-first-served within that tier. This path
//! injects as early as possible — on arm and immediately after inclusion —
//! instead of waiting for a block announce like [`crate::processor`].

use crate::control::{BotControl, InjectMode};
use crate::transact::{PrebuiltTx, TxConfig, fetch_nonce, prebuild, send};
use fp_rpc::EthereumRuntimeRPCApi;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use node_subtensor_runtime::opaque::Block;
use sc_client_api::BlockchainEvents;
use sc_transaction_pool_api::{TransactionPool, error::IntoPoolError};
use sp_core::U256;
use std::sync::Arc;
use std::time::Duration;

struct PendingTx {
    tx: PrebuiltTx,
    nonce: U256,
}

/// Spawn the pool-front injector background task.
pub fn start_pool_injector<C, P>(
    task_manager: &sc_service::TaskManager,
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    propagator: TxPropagator,
) where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + BlockchainEvents<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block> + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-pool-injector",
        None,
        run(client, pool, control, propagator),
    );
}

fn build_tx_at(cfg: &TxConfig, nonce: U256) -> PendingTx {
    PendingTx {
        tx: prebuild(cfg, nonce, vec![0u8; 4]),
        nonce,
    }
}

async fn wait_for_pending<C>(client: &C, cfg: &TxConfig) -> PendingTx
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    loop {
        match fetch_nonce(client, cfg.from) {
            Ok(nonce) => return build_tx_at(cfg, nonce),
            Err(e) => {
                log::debug!(
                    target: "bot::pool_inject",
                    "waiting for runtime before prebuild: {e}",
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn resync_pending<C>(client: &C, cfg: &TxConfig) -> Result<PendingTx, String>
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    let nonce = fetch_nonce(client, cfg.from)?;
    Ok(build_tx_at(cfg, nonce))
}

fn advance_pending(cfg: &TxConfig, next_nonce: U256) -> PendingTx {
    build_tx_at(cfg, next_nonce)
}

async fn inject<P>(
    pool: Arc<P>,
    client: &impl sp_blockchain::HeaderBackend<Block>,
    pending: &PendingTx,
    propagator: &TxPropagator,
) -> bool
where
    P: TransactionPool<Block = Block> + 'static,
{
    let at_hash = client.info().best_hash;
    log::info!(
        target: "bot::pool_inject",
        "📌 injecting tx to pool front (nonce={}, at={:?})",
        pending.nonce,
        at_hash,
    );

    match send(
        pool,
        pending.tx.clone(),
        at_hash,
        Some(propagator.clone()),
    )
    .await {
        Ok(hash) => {
            log::info!(
                target: "bot::pool_inject",
                "✅ tx in ready pool, hash = {:?}",
                hash
            );
            true
        }
        Err(e) => match e.into_pool_error() {
            Ok(sc_transaction_pool_api::error::Error::AlreadyImported(_)) => {
                log::info!(
                    target: "bot::pool_inject",
                    "✅ tx already in ready pool (nonce={})",
                    pending.nonce
                );
                true
            }
            Ok(other) => {
                log::error!(
                    target: "bot::pool_inject",
                    "❌ pool injection failed (nonce={}): {other}",
                    pending.nonce,
                );
                false
            }
            Err(e) => {
                log::error!(
                    target: "bot::pool_inject",
                    "❌ pool injection failed (nonce={}): {e}",
                    pending.nonce,
                );
                false
            }
        },
    }
}

fn run<C, P>(
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
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
    P: TransactionPool<Block = Block> + 'static,
{
    async move {
        let cfg = TxConfig::from_env();
        let mut pending = wait_for_pending(client.as_ref(), &cfg).await;
        let mut queued_nonce: Option<U256> = None;

        log::info!(
            target: "bot::pool_inject",
            "✅ initial tx pre-built, nonce={} (pool-front idle — call bot_startTxsFront)",
            pending.nonce
        );

        let mut imports = client.import_notification_stream().fuse();
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_import = imports.next() => {
                    if maybe_import.is_none() {
                        log::warn!(target: "bot::pool_inject", "import stream ended");
                        break;
                    }
                }
                _ = tick.tick() => {}
            }

            if control.inject_mode() != InjectMode::PoolFront || !control.should_send() {
                continue;
            }

            if control.take_resync() {
                match resync_pending(client.as_ref(), &cfg) {
                    Ok(p) => {
                        pending = p;
                        queued_nonce = None;
                        log::info!(
                            target: "bot::pool_inject",
                            "✅ tx re-synced on start_txs, nonce={}",
                            pending.nonce
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            target: "bot::pool_inject",
                            "⚠️ nonce resync failed: {e}",
                        );
                        continue;
                    }
                }
            }

            let chain_nonce = match fetch_nonce(client.as_ref(), cfg.from) {
                Ok(n) => n,
                Err(e) => {
                    log::debug!(
                        target: "bot::pool_inject",
                        "nonce fetch failed: {e}",
                    );
                    continue;
                }
            };

            if chain_nonce > pending.nonce {
                if queued_nonce == Some(pending.nonce) {
                    control.on_sent();
                    log::info!(
                        target: "bot::pool_inject",
                        "✅ tx included, nonce={} (remaining={:?})",
                        pending.nonce,
                        control.tx_remaining(),
                    );
                }
                pending = advance_pending(&cfg, chain_nonce);
                queued_nonce = None;
                log::info!(
                    target: "bot::pool_inject",
                    "✅ tx pre-built for next inject, nonce={}",
                    pending.nonce,
                );
            }

            if queued_nonce == Some(pending.nonce) {
                continue;
            }

            if inject(pool.clone(), client.as_ref(), &pending, &propagator).await {
                queued_nonce = Some(pending.nonce);
            }
        }
    }
    .boxed()
}
