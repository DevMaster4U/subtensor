//! JSON-RPC control interface for the bot.

use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorObjectOwned};

use crate::auto_filter::AutoFilterControl;
use crate::control::{BotControl, InjectMode};
use crate::mempool::MempoolWatcherControl;
use crate::tx_propagation::TxPropagationControl;
use crate::peers::{
    KeepTopPeersResult, PeerRecommendation, PeerPruner, PeerStat, PeerTracker,
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
    pub tx_propagation_only_bootnode: bool,
}

#[rpc(server)]
pub trait BotApi {
    /// Arm the bot. Sending begins only after [`Self::start_txs`].
    #[method(name = "bot_start")]
    fn start(&self) -> RpcResult<bool>;

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

    /// Propagate txs to bootnodes first, then to remaining full-node peers.
    #[method(name = "bot_enableTxPropagationOnlyBootnode")]
    fn enable_tx_propagation_only_bootnode(&self) -> RpcResult<bool>;

    /// Propagate txs to all full-node peers in one round (default).
    #[method(name = "bot_disableTxPropagationOnlyBootnode")]
    fn disable_tx_propagation_only_bootnode(&self) -> RpcResult<bool>;
}

pub struct BotRpc {
    control: Arc<BotControl>,
    auto_filter: Arc<AutoFilterControl>,
    mempool_watcher: Arc<MempoolWatcherControl>,
    tx_propagation: Arc<TxPropagationControl>,
    peer_tracker: Arc<PeerTracker>,
    peer_pruner: Arc<PeerPruner>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
}

impl BotRpc {
    pub fn new(
        control: Arc<BotControl>,
        auto_filter: Arc<AutoFilterControl>,
        mempool_watcher: Arc<MempoolWatcherControl>,
        tx_propagation: Arc<TxPropagationControl>,
        peer_tracker: Arc<PeerTracker>,
        peer_pruner: Arc<PeerPruner>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    ) -> Self {
        Self {
            control,
            auto_filter,
            mempool_watcher,
            tx_propagation,
            peer_tracker,
            peer_pruner,
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
        self.control.start();
        Ok(true)
    }

    fn stop(&self) -> RpcResult<bool> {
        self.control.stop();
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

        Ok(BotStatus {
            running: self.control.is_running(),
            tx_remaining: self.control.tx_remaining(),
            tx_sent: self.control.tx_sent(),
            inject_mode: match self.control.inject_mode() {
                InjectMode::OnAnnounce => "announce".into(),
                InjectMode::PoolFront => "pool_front".into(),
                InjectMode::Hybrid => "fast".into(),
            },
            auto_filter: auto,
            mempool_watcher: self.mempool_watcher.is_running(),
            tx_propagation_only_bootnode: self.tx_propagation.only_bootnode(),
        })
    }

    fn peer_stats(&self, limit: Option<u32>) -> RpcResult<Vec<PeerStat>> {
        let limit = limit.unwrap_or(20).clamp(1, 200) as usize;
        let addrs = self.peer_addrs();
        Ok(self.peer_tracker.top_peers(limit, Some(&addrs)))
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

    fn enable_tx_propagation_only_bootnode(&self) -> RpcResult<bool> {
        self.tx_propagation.enable_only_bootnode();
        Ok(true)
    }

    fn disable_tx_propagation_only_bootnode(&self) -> RpcResult<bool> {
        self.tx_propagation.disable_only_bootnode();
        Ok(true)
    }
}
