//! Synchronous pool injection from the block announce validator hook.
//!
//! Runs inside `BlockAnnounceValidator::validate()` before async validation,
//! eliminating broadcast-channel latency for announce-triggered submits.

use crate::control::{BotControl, InjectMode};
use crate::inject_shared::{InjectResult, SharedInjectState, build_tx_at, inject_sync, resync_pending};
use crate::transact::{TxConfig, TxPropagator};
use fp_rpc::EthereumRuntimeRPCApi;
use node_subtensor_runtime::opaque::Block;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_blockchain::HeaderBackend;
use sp_runtime::traits::Block as BlockT;
use std::sync::{Arc, RwLock};

trait SyncInjectInner: Send + Sync {
    fn on_announce(&self, block_number: u32);
}

/// Handle installed after the network stack is built (needs the tx propagator).
pub struct SyncInjectHandle {
    inner: RwLock<Option<Box<dyn SyncInjectInner>>>,
}

impl SyncInjectHandle {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(None),
        })
    }

    pub fn install<C, P>(
        &self,
        client: Arc<C>,
        pool: Arc<P>,
        control: Arc<BotControl>,
        state: Arc<SharedInjectState>,
        propagator: TxPropagator,
        cfg: TxConfig,
    ) where
        C: sp_api::ProvideRuntimeApi<Block>
            + HeaderBackend<Block>
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
            + Send
            + Sync
            + 'static,
    {
        *self.inner.write().expect("poisoned") = Some(Box::new(SyncAnnounceInject {
            client,
            pool,
            control,
            state,
            propagator,
            cfg,
        }));
        log::info!(target: "bot::sync_inject", "✅ sync announce injector installed");
    }

    pub fn on_announce(&self, block_number: u32) {
        let guard = self.inner.read().expect("poisoned");
        if let Some(inject) = guard.as_ref() {
            inject.on_announce(block_number);
        }
    }
}

struct SyncAnnounceInject<C, P> {
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    propagator: TxPropagator,
    cfg: TxConfig,
}

impl<C, P> SyncInjectInner for SyncAnnounceInject<C, P>
where
    C: sp_api::ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >,
{
    fn on_announce(&self, block_number: u32) {
        if !self.control.should_send() {
            return;
        }

        let mode = self.control.inject_mode();
        if !mode.uses_sync_announce_inject() {
            return;
        }

        if !self.state.is_ready() || self.state.inject_paused() {
            return;
        }

        if self.control.take_resync() {
            match resync_pending(self.client.as_ref(), &self.cfg) {
                Ok(p) => {
                    self.state.resync_pending(p.clone());
                    log::info!(
                        target: "bot::sync_inject",
                        "✅ tx re-synced on start, nonce={}",
                        p.nonce,
                    );
                }
                Err(e) => {
                    log::warn!(target: "bot::sync_inject", "⚠️ nonce resync failed: {e}");
                    return;
                }
            }
        }

        let once_per_block =
            matches!(mode, InjectMode::OnAnnounce | InjectMode::PoolFront);
        if once_per_block && !self.state.try_claim_announce_block(block_number) {
            return;
        }

        let Some(pending) = self.state.pending() else {
            return;
        };

        let at_hash = self.client.info().best_hash;
        match inject_sync(
            self.pool.as_ref(),
            at_hash,
            &pending,
            &self.propagator,
        ) {
            InjectResult::Queued => match mode {
                InjectMode::OnAnnounce => {
                    self.state.mark_queued(pending.nonce);
                    self.control.on_sent();
                    let next = pending.nonce.saturating_add(1u32.into());
                    let advanced = build_tx_at(&self.cfg, next);
                    self.state.set_pending(advanced.clone());
                    self.state.clear_queued();
                    log::info!(
                        target: "bot::sync_inject",
                        "✅ announce inject ok, nonce={} -> next={} (remaining={:?})",
                        pending.nonce,
                        advanced.nonce,
                        self.control.tx_remaining(),
                    );
                }
                InjectMode::PoolFront => {
                    self.state.mark_queued(pending.nonce);
                    log::info!(
                        target: "bot::sync_inject",
                        "✅ pool-front announce inject ok, nonce={} at block #{block_number} (remaining={:?})",
                        pending.nonce,
                        remaining = self.control.tx_remaining(),
                    );
                }
                InjectMode::Hybrid => {
                    self.state.mark_queued(pending.nonce);
                    log::info!(
                        target: "bot::sync_inject",
                        "✅ fast refresh ok, nonce={} at block #{block_number}",
                        pending.nonce,
                    );
                }
            },
            InjectResult::Fatal => {
                self.state.set_inject_paused(true);
                log::error!(
                    target: "bot::sync_inject",
                    "❌ sync inject halted (nonce={}): insufficient funds or banned",
                    pending.nonce,
                );
            }
            InjectResult::Retry => {
                if once_per_block {
                    self.state.clear_announce_claim(block_number);
                }
                log::debug!(
                    target: "bot::sync_inject",
                    "sync inject retry later (nonce={})",
                    pending.nonce,
                );
            }
        }
    }
}
