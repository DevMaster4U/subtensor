//! Wall-clock scheduled injection within each 12-second slot.
//!
//! With `delay_ms = 300`, fires at 0.3s, 12.3s, 24.3s, 36.3s, 48.3s … (epoch-aligned).

use crate::announce_timing::MOD12_MS;
use crate::control::{BotControl, InjectMode};
use crate::inject_shared::{InjectResult, SharedInjectState, inject_sync, resync_pending};
use crate::transact::{TxConfig, TxPropagator};
use fp_rpc::EthereumRuntimeRPCApi;
use futures::FutureExt;
use node_subtensor_runtime::opaque::Block;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_blockchain::HeaderBackend;
use sp_runtime::traits::Block as BlockT;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const FIRE_WINDOW_MS: u64 = 80;

fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn start_time_injector<C, P>(
    task_manager: &sc_service::TaskManager,
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    propagator: TxPropagator,
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
        + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-time-injector",
        None,
        run(client, pool, control, state, propagator),
    );
}

fn run<C, P>(
    client: Arc<C>,
    pool: Arc<P>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    propagator: TxPropagator,
) -> futures::future::BoxFuture<'static, ()>
where
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
        + 'static,
{
    async move {
        let cfg = TxConfig::from_env();
        let mut last_fired_slot: Option<u64> = None;

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            if control.inject_mode() != InjectMode::ScheduledTime {
                last_fired_slot = None;
                continue;
            }

            if !control.should_send() || !control.is_running() {
                last_fired_slot = None;
                continue;
            }

            let delay_ms = match control.schedule_delay_ms() {
                Some(d) => u64::from(d),
                None => continue,
            };

            if delay_ms >= MOD12_MS {
                log::warn!(
                    target: "bot::time_inject",
                    "delay_ms={delay_ms} >= {MOD12_MS}; clamping to slot start",
                );
            }
            let delay_ms = delay_ms.min(MOD12_MS - 1);

            let now_ms = unix_ms(SystemTime::now());
            let slot = now_ms / MOD12_MS;
            let into_slot = now_ms % MOD12_MS;

            if into_slot < delay_ms || into_slot >= delay_ms + FIRE_WINDOW_MS {
                continue;
            }

            if last_fired_slot == Some(slot) {
                continue;
            }
            last_fired_slot = Some(slot);

            if !state.is_ready() || state.inject_paused() {
                continue;
            }

            if control.take_resync() {
                match resync_pending(client.as_ref(), &cfg) {
                    Ok(p) => {
                        state.resync_pending(p.clone());
                        log::info!(
                            target: "bot::time_inject",
                            "✅ tx re-synced on start_with_time, nonce={}",
                            p.nonce,
                        );
                    }
                    Err(e) => {
                        log::warn!(target: "bot::time_inject", "⚠️ nonce resync failed: {e}");
                        last_fired_slot = None;
                        continue;
                    }
                }
            }

            let Some(pending) = state.pending() else {
                last_fired_slot = None;
                continue;
            };

            let at_hash = client.info().best_hash;
            let slot_sec = slot as f64 * 12.0 + delay_ms as f64 / 1000.0;
            log::info!(
                target: "bot::time_inject",
                "⏱ scheduled inject slot={slot_sec:.1}s (nonce={}, delay_ms={})",
                pending.nonce,
                delay_ms,
            );

            match inject_sync(pool.as_ref(), at_hash, &pending, &propagator) {
                InjectResult::Queued => {
                    control.on_sent();
                    state.mark_queued(pending.nonce);
                    let next = pending.nonce.saturating_add(1u32.into());
                    let advanced = crate::inject_shared::build_tx_at(&cfg, next);
                    state.set_pending(advanced.clone());
                    state.clear_queued();
                    log::info!(
                        target: "bot::time_inject",
                        "✅ scheduled inject ok, nonce={} -> next={} (remaining={:?})",
                        pending.nonce,
                        advanced.nonce,
                        control.tx_remaining(),
                    );
                }
                InjectResult::Fatal => {
                    state.set_inject_paused(true);
                    log::error!(
                        target: "bot::time_inject",
                        "❌ scheduled inject halted (nonce={})",
                        pending.nonce,
                    );
                }
                InjectResult::Retry => {
                    last_fired_slot = None;
                }
            }
        }
    }
    .boxed()
}
