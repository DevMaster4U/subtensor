//! Block processor.
//!
//! `start_bot` is the single entry point called from `service.rs`.
//! It listens for pre-import new-best block announcements from the network
//! layer and submits transactions when the runtime control is active.

use crate::announce::BlockAnnounceNotification;
use crate::authority_peers::{AuthorityPeerRegistry, correlate_block_author};
use crate::control::{BotControl, InjectMode};
use crate::inject_shared::{SharedInjectState, build_tx_at, resync_pending};
use crate::peers::PeerTracker;
use crate::propagation_tracker::PropagationTracker;
use crate::transact::{TxConfig, TxPropagator, fetch_nonce, send};
use fp_rpc::EthereumRuntimeRPCApi;
use futures::{FutureExt, future::BoxFuture};
use node_subtensor_runtime::opaque::Block;
use sc_network::NetworkStatusProvider;
use sc_network_sync::SyncingService;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool, error::IntoPoolError};
use sp_consensus_aura::{AuraApi, sr25519::AuthorityId as AuraId};
use sp_runtime::traits::{Block as BlockT, SaturatedConversion};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

/// Spawn the bot background task.
pub fn start_bot<C, P>(
    task_manager: &sc_service::TaskManager,
    client: Arc<C>,
    pool: Arc<P>,
    sync: Arc<SyncingService<Block>>,
    announce_rx: broadcast::Receiver<BlockAnnounceNotification>,
    control: Arc<BotControl>,
    state: Arc<SharedInjectState>,
    peer_tracker: Arc<PeerTracker>,
    propagation_tracker: Arc<PropagationTracker>,
    authority_registry: Arc<AuthorityPeerRegistry>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    propagator: TxPropagator,
) where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block> + AuraApi<Block, AuraId>,
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >
        + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-processor",
        None,
        run(
            client,
            pool,
            sync,
            announce_rx,
            control,
            state,
            peer_tracker,
            propagation_tracker,
            authority_registry,
            network,
            propagator,
        ),
    );
}

async fn wait_for_pending<C>(client: &C, cfg: &TxConfig, state: &SharedInjectState)
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    if state.is_ready() {
        return;
    }
    loop {
        match fetch_nonce(client, cfg.from) {
            Ok(nonce) => {
                state.init_pending(build_tx_at(cfg, nonce));
                return;
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

fn record_peer_candidates<C>(
    client: Arc<C>,
    sync: Arc<SyncingService<Block>>,
    tracker: Arc<PeerTracker>,
    registry: Arc<AuthorityPeerRegistry>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    block_number: u32,
    parent_hash: <Block as BlockT>::Hash,
    slot: Option<u64>,
    block_hash: <Block as BlockT>::Hash,
    announcing_peer: Option<String>,
) where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    C::Api: AuraApi<Block, AuraId>,
{
    tokio::spawn(async move {
        let addrs = network.connected_peer_addresses().await;
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
                tracker.record_announce(
                    block_number,
                    rows,
                    announcing_peer.as_deref(),
                );

                let attributed_peer = announcing_peer
                    .or_else(|| tracker.first_peer_for_block(block_number));
                if let Some(first_peer) = attributed_peer {
                    let roles = tracker
                        .lookup(&first_peer)
                        .map(|(_, _, _, _, r)| r)
                        .unwrap_or_default();
                    let multiaddr = addrs.get(&first_peer).cloned();
                    correlate_block_author(
                        client,
                        registry,
                        block_number,
                        parent_hash,
                        slot,
                        block_hash,
                        first_peer,
                        roles,
                        multiaddr,
                    )
                    .await;
                }
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
    state: Arc<SharedInjectState>,
    peer_tracker: Arc<PeerTracker>,
    propagation_tracker: Arc<PropagationTracker>,
    authority_registry: Arc<AuthorityPeerRegistry>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    propagator: TxPropagator,
) -> BoxFuture<'static, ()>
where
    C: sp_api::ProvideRuntimeApi<Block>
        + sp_blockchain::HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    C::Api: EthereumRuntimeRPCApi<Block> + AuraApi<Block, AuraId>,
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
                target: "bot::processor",
                "✅ initial tx pre-built, nonce={} (stopped — call bot_startTxs)",
                pending.nonce
            );
        }

        let mut last_tracked_at_number = None;

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
                propagation_tracker.record_announce(
                    notification.number,
                    notification.announcing_peer.clone(),
                );
                record_peer_candidates(
                    client.clone(),
                    sync.clone(),
                    peer_tracker.clone(),
                    authority_registry.clone(),
                    network.clone(),
                    notification.number,
                    notification.parent_hash,
                    notification.slot,
                    notification.hash,
                    notification.announcing_peer.clone(),
                );
            }

            if !control.should_send() {
                continue;
            }

            let mode = control.inject_mode();
            if mode == InjectMode::PoolFront
                || mode == InjectMode::Hybrid
                || mode == InjectMode::ScheduledTime
            {
                continue;
            }

            // Sync hook handles OnAnnounce primary path; processor is fallback.
            if !state.try_claim_announce_block(notification.number) {
                continue;
            }

            if control.take_resync() {
                match resync_pending(client.as_ref(), &cfg) {
                    Ok(p) => {
                        state.resync_pending(p.clone());
                        log::info!(
                            target: "bot::processor",
                            "✅ tx re-synced on start_txs, nonce={}",
                            p.nonce
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            target: "bot::processor",
                            "⚠️ nonce resync failed: {e}",
                        );
                        state.clear_announce_claim(notification.number);
                        continue;
                    }
                }
            }

            let Some(pending) = state.pending() else {
                state.clear_announce_claim(notification.number);
                continue;
            };

            let at_hash = client.info().best_hash;
            log::info!(
                target: "bot::processor",
                "🚀 fallback announce inject #{} (nonce={}, at={:?})",
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
                let next = pending.nonce.saturating_add(1u32.into());
                let advanced = build_tx_at(&cfg, next);
                state.set_pending(advanced.clone());
                log::info!(
                    target: "bot::processor",
                    "✅ tx pre-built for next send, nonce={} (remaining={:?})",
                    advanced.nonce,
                    control.tx_remaining(),
                );
            } else {
                state.clear_announce_claim(notification.number);
            }
        }
    }
    .boxed()
}
