//! JSON-RPC control interface for the bot.

use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorObjectOwned};

use crate::authorities::{AuraAuthority, AuraSchedule};
use crate::authority_discovery::AuthorityRpcBackend;
use crate::authority_peers::{ApplyAuthorityReservedResult, AuthorityPeerMapping, ConnectedAuthorityPeer};
use crate::auto_filter::AutoFilterControl;
use crate::announce_timing::AnnounceTimingTracker;
use crate::control::{BotControl, InjectMode};
use crate::propagation_tracker::{OwnPropagationRecord, PropagationTracker};
use crate::tx_propagation::TxPropagationControl;
use crate::peers::{
    KeepTopPeersResult, NetworkPeerRow, PeerRecommendation, PeerPruner, PeerStat, PeerTracker,
    SetReservedPeersResult, TxGossipCheck,
};
use sc_network::NetworkStatusProvider;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AutoFilterStatus {
    pub running: bool,
    pub interval_secs: Option<u64>,
    pub keep_count: Option<u32>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BotStatus {
    pub running: bool,
    /// `None` means unlimited sends are configured.
    pub tx_remaining: Option<u32>,
    pub tx_sent: u32,
    pub inject_mode: String,
    pub auto_filter: AutoFilterStatus,
    pub mempool_watcher: bool,
    pub tx_propagation_first_reserved_node: bool,
    /// Outbound send limit: max ranked peers to gossip to per round. `0` = all ranked peers.
    /// Does not limit incoming gossip.
    pub tx_propagation_max_peers: u32,
    /// Min announce offset (ms mod 12s) over the last 100 blocks, after `bot_start`.
    pub min_value: Option<u32>,
    /// Average announce offset (ms mod 12s) over the last 100 blocks.
    pub average_value: Option<f64>,
    /// Active scheduled-inject offset when mode is `scheduled_time`.
    pub schedule_delay_ms: Option<u32>,
}

#[rpc(server)]
pub trait BotApi {
    /// Arm the bot. Sending begins only after [`Self::start_txs`].
    /// Enables announce mod12 timing collection (last 100 blocks).
    #[method(name = "bot_start")]
    fn start(&self) -> RpcResult<bool>;

    /// Send `tx_count` txs at `delay_ms` into each 12-second wall-clock slot.
    /// `delay_ms = 300` → 0.3s, 12.3s, 24.3s, 36.3s, 48.3s, …
    #[method(name = "bot_startWithTime")]
    fn start_with_time(&self, tx_count: u32, delay_ms: u32) -> RpcResult<bool>;

    /// Stop the bot immediately.
    #[method(name = "bot_stop")]
    fn stop(&self) -> RpcResult<bool>;

    /// Begin sending `tx_count` transactions on block announces.
    /// `tx_count = 0` means unlimited while running.
    #[method(name = "bot_startTxs")]
    fn start_txs(&self, tx_count: u32) -> RpcResult<bool>;

    /// Pre-submit `tx_count` transactions to the front of the ready pool.
    /// Uses early injection instead of waiting for block announces.
    /// `tx_count = 0` means unlimited while running.
    #[method(name = "bot_startTxsFront")]
    fn start_txs_front(&self, tx_count: u32) -> RpcResult<bool>;

    /// Pool-front plus announce refresh on every header (FCFS race mode).
    /// `tx_count = 0` means unlimited while running.
    #[method(name = "bot_startTxsFast")]
    fn start_txs_fast(&self, tx_count: u32) -> RpcResult<bool>;

    /// Alias for [`Self::start_txs_fast`].
    #[method(name = "bot_startTxsHybrid")]
    fn start_txs_hybrid(&self, tx_count: u32) -> RpcResult<bool>;

    /// Current bot status.
    #[method(name = "bot_status")]
    fn status(&self) -> RpcResult<BotStatus>;

    /// Leaderboard of peers correlated with early block announces and tx propagation.
    #[method(name = "bot_peerStats")]
    fn peer_stats(&self, limit: Option<u32>) -> RpcResult<Vec<PeerStat>>;

    /// Connected and previously seen peers with multiaddrs (from network state).
    #[method(name = "bot_networkPeers")]
    fn network_peers(&self) -> RpcResult<Vec<NetworkPeerRow>>;

    /// Tx propagation scoring health check (confirms new binary + hit totals).
    #[method(name = "bot_checkTxGossip")]
    fn check_tx_gossip(&self, top: Option<u32>) -> RpcResult<TxGossipCheck>;

    /// Top peers to investigate for `--reserved-peers`.
    #[method(name = "bot_peerRecommendations")]
    fn peer_recommendations(&self, limit: Option<u32>) -> RpcResult<Vec<PeerRecommendation>>;

    /// Keep the top `keep_count` connected peers (by combined announce + tx score) and disconnect the rest.
    #[method(name = "bot_keepTopPeers")]
    fn keep_top_peers(&self, keep_count: u32) -> RpcResult<KeepTopPeersResult>;

    /// Replace all reserved peers with multiaddrs from a text file (one per line).
    #[method(name = "bot_setReservedPeersFromFile")]
    fn set_reserved_peers_from_file(&self, path: String) -> RpcResult<SetReservedPeersResult>;

    /// Start periodic peer filtering: keep top `keep_count` every `interval_secs`.
    #[method(name = "bot_startAutoFilter")]
    fn start_auto_filter(&self, interval_secs: u64, keep_count: u32) -> RpcResult<bool>;

    /// Stop periodic peer filtering.
    #[method(name = "bot_stopAutoFilter")]
    fn stop_auto_filter(&self) -> RpcResult<bool>;

    /// Enable the ready-pool mempool watcher (logs imports to node logs).
    #[method(name = "bot_enableMempoolWatcher")]
    fn enable_mempool_watcher(&self) -> RpcResult<bool>;

    /// Disable the ready-pool mempool watcher.
    #[method(name = "bot_disableMempoolWatcher")]
    fn disable_mempool_watcher(&self) -> RpcResult<bool>;

    /// Propagate txs to the first `--reserved-nodes` peer first, then remaining full-node peers.
    #[method(name = "bot_enableTxPropagationFirstReservedNode")]
    fn enable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool>;

    /// Propagate txs to all full-node peers in one round (default).
    #[method(name = "bot_disableTxPropagationFirstReservedNode")]
    fn disable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool>;

    /// Outbound-only: send tx gossip to at most `max` ranked peers per round.
    /// Incoming gossip is unchanged (accept from all connected tx peers). `0` = no send limit.
    #[method(name = "bot_setTxPropagationMaxPeers")]
    fn set_tx_propagation_max_peers(&self, max: u32) -> RpcResult<bool>;

    /// On-chain Aura authority set (block producer public keys / accounts).
    #[method(name = "bot_auraAuthorities")]
    fn aura_authorities(&self) -> RpcResult<Vec<AuraAuthority>>;

    /// Current Aura slot + authority set + predicted next authors.
    #[method(name = "bot_auraSchedule")]
    fn aura_schedule(&self, upcoming: Option<u32>) -> RpcResult<AuraSchedule>;

    /// Learned `{ Aura account → peer }` mappings from block announce correlation.
    #[method(name = "bot_authorityPeers")]
    fn authority_peers(&self) -> RpcResult<Vec<AuthorityPeerMapping>>;

    /// Connected peers advertising AUTHORITY role (enriched with learned mapping).
    #[method(name = "bot_connectedAuthorityPeers")]
    fn connected_authority_peers(&self) -> RpcResult<Vec<ConnectedAuthorityPeer>>;

    /// Write learned authority multiaddrs to a reserved-peers file.
    #[method(name = "bot_exportAuthorityReserved")]
    fn export_authority_reserved(&self, path: String, min_hits: Option<u64>) -> RpcResult<Vec<String>>;

    /// Add learned authority peers (min hits) as network reserved peers.
    #[method(name = "bot_applyAuthorityReserved")]
    fn apply_authority_reserved(&self, min_hits: Option<u64>) -> RpcResult<ApplyAuthorityReservedResult>;

    /// Latest bot-initiated tx propagation record (announce context + peer send order).
    #[method(name = "bot_ownPropagationLatest")]
    fn own_propagation_latest(&self) -> RpcResult<Option<OwnPropagationRecord>>;

    /// Recent bot-initiated tx propagation records (newest first).
    #[method(name = "bot_ownPropagationHistory")]
    fn own_propagation_history(&self, limit: Option<u32>) -> RpcResult<Vec<OwnPropagationRecord>>;
}

pub struct BotRpc {
    control: Arc<BotControl>,
    announce_timing: Arc<AnnounceTimingTracker>,
    auto_filter: Arc<AutoFilterControl>,
    mempool_watcher: Arc<MempoolWatcherControl>,
    tx_propagation: Arc<TxPropagationControl>,
    peer_tracker: Arc<PeerTracker>,
    propagation_tracker: Arc<PropagationTracker>,
    peer_pruner: Arc<PeerPruner>,
    authority: Arc<dyn AuthorityRpcBackend>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
}

impl BotRpc {
    pub fn new(
        control: Arc<BotControl>,
        announce_timing: Arc<AnnounceTimingTracker>,
        auto_filter: Arc<AutoFilterControl>,
        mempool_watcher: Arc<MempoolWatcherControl>,
        tx_propagation: Arc<TxPropagationControl>,
        peer_tracker: Arc<PeerTracker>,
        propagation_tracker: Arc<PropagationTracker>,
        peer_pruner: Arc<PeerPruner>,
        authority: Arc<dyn AuthorityRpcBackend>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    ) -> Self {
        Self {
            control,
            announce_timing,
            auto_filter,
            mempool_watcher,
            tx_propagation,
            peer_tracker,
            propagation_tracker,
            peer_pruner,
            authority,
            network,
        }
    }

    fn peer_addrs(&self) -> std::collections::HashMap<String, String> {
        let network = Arc::clone(&self.network);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                network.connected_peer_addresses().await
            })
        })
    }
}

impl BotApiServer for BotRpc {
    fn start(&self) -> RpcResult<bool> {
        self.announce_timing.enable();
        self.control.start();
        Ok(true)
    }

    fn start_with_time(&self, tx_count: u32, delay_ms: u32) -> RpcResult<bool> {
        self.announce_timing.enable();
        self.control.start_with_time(tx_count, delay_ms);
        Ok(true)
    }

    fn stop(&self) -> RpcResult<bool> {
        self.control.stop();
        self.announce_timing.disable();
        Ok(true)
    }

    fn start_txs(&self, tx_count: u32) -> RpcResult<bool> {
        self.control.start_txs(tx_count);
        Ok(true)
    }

    fn start_txs_front(&self, tx_count: u32) -> RpcResult<bool> {
        self.control.start_txs_pool_front(tx_count);
        Ok(true)
    }

    fn start_txs_fast(&self, tx_count: u32) -> RpcResult<bool> {
        self.control.start_txs_hybrid(tx_count);
        Ok(true)
    }

    fn start_txs_hybrid(&self, tx_count: u32) -> RpcResult<bool> {
        self.start_txs_fast(tx_count)
    }

    fn status(&self) -> RpcResult<BotStatus> {
        let auto = self.auto_filter.config().map(|cfg| AutoFilterStatus {
            running: true,
            interval_secs: Some(cfg.interval_secs),
            keep_count: Some(cfg.keep_count),
        }).unwrap_or(AutoFilterStatus {
            running: false,
            interval_secs: None,
            keep_count: None,
        });

        let (min_value, average_value) = self.announce_timing.stats();

        Ok(BotStatus {
            running: self.control.is_running(),
            tx_remaining: self.control.tx_remaining(),
            tx_sent: self.control.tx_sent(),
            inject_mode: match self.control.inject_mode() {
                InjectMode::OnAnnounce => "announce".into(),
                InjectMode::PoolFront => "pool_front".into(),
                InjectMode::Hybrid => "fast".into(),
                InjectMode::ScheduledTime => "scheduled_time".into(),
            },
            auto_filter: auto,
            mempool_watcher: self.mempool_watcher.is_running(),
            tx_propagation_first_reserved_node: self.tx_propagation.first_reserved_node(),
            tx_propagation_max_peers: self.tx_propagation.max_propagation_peers(),
            min_value,
            average_value,
            schedule_delay_ms: self.control.schedule_delay_ms(),
        })
    }

    fn peer_stats(&self, limit: Option<u32>) -> RpcResult<Vec<PeerStat>> {
        let limit = limit.unwrap_or(20).clamp(1, 200) as usize;
        let addrs = self.peer_addrs();
        Ok(self.peer_tracker.top_peers(limit, Some(&addrs)))
    }

    fn network_peers(&self) -> RpcResult<Vec<NetworkPeerRow>> {
        let pruner = Arc::clone(&self.peer_pruner);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pruner.network_peers().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn check_tx_gossip(&self, top: Option<u32>) -> RpcResult<TxGossipCheck> {
        let top = top.unwrap_or(10).clamp(1, 50) as usize;
        let addrs = self.peer_addrs();
        Ok(self.peer_tracker.tx_gossip_check(top, Some(&addrs)))
    }

    fn peer_recommendations(&self, limit: Option<u32>) -> RpcResult<Vec<PeerRecommendation>> {
        let limit = limit.unwrap_or(10).clamp(1, 100) as usize;
        let addrs = self.peer_addrs();
        Ok(self
            .peer_tracker
            .recommendations(limit, Some(&addrs)))
    }

    fn keep_top_peers(&self, keep_count: u32) -> RpcResult<KeepTopPeersResult> {
        let pruner = Arc::clone(&self.peer_pruner);
        // RPC handlers run on the node's tokio runtime; block_in_place allows
        // calling async Substrate APIs without nesting runtimes (which panics).
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                pruner.keep_top(keep_count).await
            })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn set_reserved_peers_from_file(&self, path: String) -> RpcResult<SetReservedPeersResult> {
        let pruner = Arc::clone(&self.peer_pruner);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                pruner.set_reserved_from_file(&path).await
            })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn start_auto_filter(&self, interval_secs: u64, keep_count: u32) -> RpcResult<bool> {
        self.auto_filter.start(interval_secs, keep_count);
        Ok(true)
    }

    fn stop_auto_filter(&self) -> RpcResult<bool> {
        self.auto_filter.stop();
        Ok(true)
    }

    fn enable_mempool_watcher(&self) -> RpcResult<bool> {
        self.mempool_watcher.start();
        Ok(true)
    }

    fn disable_mempool_watcher(&self) -> RpcResult<bool> {
        self.mempool_watcher.stop();
        Ok(true)
    }

    fn enable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool> {
        self.tx_propagation.enable_first_reserved_node();
        Ok(true)
    }

    fn disable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool> {
        self.tx_propagation.disable_first_reserved_node();
        Ok(true)
    }

    fn set_tx_propagation_max_peers(&self, max: u32) -> RpcResult<bool> {
        self.tx_propagation.set_max_propagation_peers(max);
        Ok(true)
    }

    fn aura_authorities(&self) -> RpcResult<Vec<AuraAuthority>> {
        self.authority
            .aura_authorities()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn aura_schedule(&self, upcoming: Option<u32>) -> RpcResult<AuraSchedule> {
        let upcoming = upcoming.unwrap_or(5).clamp(1, 32);
        self.authority
            .aura_schedule(upcoming)
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn authority_peers(&self) -> RpcResult<Vec<AuthorityPeerMapping>> {
        Ok(self.authority.authority_peer_mappings())
    }

    fn connected_authority_peers(&self) -> RpcResult<Vec<ConnectedAuthorityPeer>> {
        self.authority
            .connected_authority_peers()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn export_authority_reserved(&self, path: String, min_hits: Option<u64>) -> RpcResult<Vec<String>> {
        let min_hits = min_hits.unwrap_or(3);
        self.authority
            .export_authority_reserved(&path, min_hits)
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn apply_authority_reserved(&self, min_hits: Option<u64>) -> RpcResult<ApplyAuthorityReservedResult> {
        let min_hits = min_hits.unwrap_or(3);
        self.authority
            .apply_authority_reserved(min_hits)
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn own_propagation_latest(&self) -> RpcResult<Option<OwnPropagationRecord>> {
        let addrs = self.peer_addrs();
        Ok(self
            .propagation_tracker
            .latest()
            .map(|r| PropagationTracker::enrich_record(r, &addrs)))
    }

    fn own_propagation_history(&self, limit: Option<u32>) -> RpcResult<Vec<OwnPropagationRecord>> {
        let limit = limit.unwrap_or(20).clamp(1, 100) as usize;
        let addrs = self.peer_addrs();
        Ok(self
            .propagation_tracker
            .history(limit)
            .into_iter()
            .map(|r| PropagationTracker::enrich_record(r, &addrs))
            .collect())
    }
}
