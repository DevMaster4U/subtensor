//! Block processor.
//!
//! `start_bot` is the single entry point called from `service.rs`.
//! It listens for pre-import new-best block announcements from the network
//! layer and submits transactions when the runtime control is active.

use crate::announce::BlockAnnounceNotification;
use crate::control::{BotControl, InjectMode};
use crate::peers::PeerTracker;
use crate::transact::{PrebuiltTx, TxConfig, TxPropagator, fetch_nonce, prebuild, send};
use fp_rpc::EthereumRuntimeRPCApi;
use futures::{FutureExt, future::BoxFuture};
use node_subtensor_runtime::opaque::Block;
use sc_network_sync::SyncingService;
use sc_transaction_pool_api::{TransactionPool, error::IntoPoolError};
use sp_core::U256;
use sp_runtime::traits::SaturatedConversion;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

struct PendingTx {
    tx: PrebuiltTx,
    nonce: U256,
}

/// Spawn the bot background task.
pub fn start_bot<C, P>(
    task_manager: &sc_service::TaskManager,
    client: Arc<C>,
    pool: Arc<P>,
    sync: Arc<SyncingService<Block>>,
    announce_rx: broadcast::Receiver<BlockAnnounceNotification>,
    control: Arc<BotControl>,
    peer_tracker: Arc<PeerTracker>,
    propagator: TxPropagator,
) where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block> + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-processor",
        None,
        run(client, pool, sync, announce_rx, control, peer_tracker, propagator),
    );
}

fn build_tx_at(cfg: &TxConfig, nonce: U256) -> PendingTx {
    PendingTx {
        tx: prebuild(cfg, nonce, vec![0u8; 4]),
        nonce,
    }
}

async fn wait_for_pending<C>(client: &C, cfg: &TxConfig) -> (PendingTx, U256)
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    loop {
        match fetch_nonce(client, cfg.from) {
            Ok(nonce) => {
                let pending = build_tx_at(cfg, nonce);
                let next_nonce = nonce.saturating_add(U256::from(1));
                return (pending, next_nonce);
            }
            Err(e) => {
                log::debug!(
                    target: "bot::processor",
                    "waiting for runtime before prebuild: {e}",
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn resync_pending<C>(client: &C, cfg: &TxConfig) -> Result<(PendingTx, U256), String>
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    let nonce = fetch_nonce(client, cfg.from)?;
    let pending = build_tx_at(cfg, nonce);
    let next_nonce = nonce.saturating_add(U256::from(1));
    Ok((pending, next_nonce))
}

fn advance_pending(cfg: &TxConfig, next_nonce: U256) -> (PendingTx, U256) {
    let pending = build_tx_at(cfg, next_nonce);
    let following = next_nonce.saturating_add(U256::from(1));
    (pending, following)
}

fn record_peer_candidates(
    sync: Arc<SyncingService<Block>>,
    tracker: Arc<PeerTracker>,
    block_number: u32,
) {
    tokio::spawn(async move {
        match sync.peers_info().await {
            Ok(peers) => {
                let rows = peers
                    .into_iter()
                    .map(|(peer_id, info)| {
                        let best: u64 = info.best_number.saturated_into();
                        (
                            peer_id.to_base58(),
                            best,
                            format!("{:?}", info.roles),
                        )
                    })
                    .collect::<Vec<_>>();
                tracker.record_announce(block_number, rows);
            }
            Err(e) => {
                log::debug!(target: "bot::peers", "peers_info failed for #{block_number}: {e:?}");
            }
        }
    });
}

fn run<C, P>(
    client: Arc<C>,
    pool: Arc<P>,
    sync: Arc<SyncingService<Block>>,
    mut announce_rx: broadcast::Receiver<BlockAnnounceNotification>,
    control: Arc<BotControl>,
    peer_tracker: Arc<PeerTracker>,
    propagator: TxPropagator,
) -> BoxFuture<'static, ()>
where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block>,
    P: TransactionPool<Block = Block> + 'static,
{
    async move {
        let cfg = TxConfig::from_env();

        let (mut pending, mut next_nonce) = wait_for_pending(client.as_ref(), &cfg).await;
        log::info!(
            target: "bot::processor",
            "✅ initial tx pre-built, nonce={} (stopped — call bot_startTxs)",
            pending.nonce
        );

        let mut last_tracked_at_number = None;
        let mut last_sent_at_number = None;

        loop {
            let notification = match announce_rx.recv().await {
                Ok(n) => n,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    log::warn!(
                        target: "bot::processor",
                        "block announce receiver lagged, skipped {skipped} events",
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::warn!(target: "bot::processor", "block announce channel closed");
                    break;
                }
            };

            if last_tracked_at_number != Some(notification.number) {
                last_tracked_at_number = Some(notification.number);
                record_peer_candidates(
                    sync.clone(),
                    peer_tracker.clone(),
                    notification.number,
                );
            }

            if !control.should_send() {
                continue;
            }

            if control.inject_mode() == InjectMode::PoolFront {
                continue;
            }

            if last_sent_at_number == Some(notification.number) {
                continue;
            }
            last_sent_at_number = Some(notification.number);

            if control.take_resync() {
                match resync_pending(client.as_ref(), &cfg) {
                    Ok((p, n)) => {
                        pending = p;
                        next_nonce = n;
                        log::info!(
                            target: "bot::processor",
                            "✅ tx re-synced on start_txs, nonce={}",
                            pending.nonce
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            target: "bot::processor",
                            "⚠️ nonce resync failed: {e}",
                        );
                        continue;
                    }
                }
            }

            let at_hash = client.info().best_hash;
            log::info!(
                target: "bot::processor",
                "🚀 sending tx on announce #{} (nonce={}, at={:?})",
                notification.number,
                pending.nonce,
                at_hash,
            );

            let accepted = match send(
                pool.clone(),
                pending.tx.clone(),
                at_hash,
                Some(propagator.clone()),
            )
            .await {
                Ok(hash) => {
                    log::info!(
                        target: "bot::processor",
                        "✅ tx in pool, hash = {:?}",
                        hash
                    );
                    true
                }
                Err(e) => match e.into_pool_error() {
                    Ok(sc_transaction_pool_api::error::Error::AlreadyImported(_)) => {
                        log::info!(
                            target: "bot::processor",
                            "✅ tx already in pool (nonce={})",
                            pending.nonce
                        );
                        true
                    }
                    Ok(other) => {
                        log::error!(
                            target: "bot::processor",
                            "❌ pool submission failed (nonce={}): {other}",
                            pending.nonce,
                        );
                        false
                    }
                    Err(e) => {
                        log::error!(
                            target: "bot::processor",
                            "❌ pool submission failed (nonce={}): {e}",
                            pending.nonce,
                        );
                        false
                    }
                },
            };

            if accepted {
                control.on_sent();
                (pending, next_nonce) = advance_pending(&cfg, next_nonce);
                log::info!(
                    target: "bot::processor",
                    "✅ tx pre-built for next send, nonce={} (remaining={:?})",
                    pending.nonce,
                    control.tx_remaining(),
                );
            }
        }
    }
    .boxed()
}
