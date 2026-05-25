//! JSON-RPC control interface for the bot.

use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc};

use crate::control::{BotControl, InjectMode};
use crate::peers::{PeerRecommendation, PeerStat, PeerTracker};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BotStatus {
    pub running: bool,
    /// `None` means unlimited sends are configured.
    pub tx_remaining: Option<u32>,
    pub tx_sent: u32,
    pub inject_mode: String
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

    /// Current bot status.
    #[method(name = "bot_status")]
    fn status(&self) -> RpcResult<BotStatus>;

    /// Leaderboard of peers correlated with early block announces.
    #[method(name = "bot_peerStats")]
    fn peer_stats(&self, limit: Option<u32>) -> RpcResult<Vec<PeerStat>>;

    /// Top peers to investigate for `--reserved-peers`.
    #[method(name = "bot_peerRecommendations")]
    fn peer_recommendations(&self, limit: Option<u32>) -> RpcResult<Vec<PeerRecommendation>>;
}

pub struct BotRpc {
    control: Arc<BotControl>,
    peer_tracker: Arc<PeerTracker>,
}

impl BotRpc {
    pub fn new(control: Arc<BotControl>, peer_tracker: Arc<PeerTracker>) -> Self {
        Self {
            control,
            peer_tracker,
        }
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

    fn status(&self) -> RpcResult<BotStatus> {
        Ok(BotStatus {
            running: self.control.is_running(),
            tx_remaining: self.control.tx_remaining(),
            tx_sent: self.control.tx_sent(),
            inject_mode: match self.control.inject_mode() {
                InjectMode::OnAnnounce => "announce".into(),
                InjectMode::PoolFront => "pool_front".into(),
            },
        })
    }

    fn peer_stats(&self, limit: Option<u32>) -> RpcResult<Vec<PeerStat>> {
        let limit = limit.unwrap_or(20).clamp(1, 200) as usize;
        Ok(self.peer_tracker.top_peers(limit))
    }

    fn peer_recommendations(&self, limit: Option<u32>) -> RpcResult<Vec<PeerRecommendation>> {
        let limit = limit.unwrap_or(10).clamp(1, 100) as usize;
        Ok(self.peer_tracker.recommendations(limit))
    }
}
