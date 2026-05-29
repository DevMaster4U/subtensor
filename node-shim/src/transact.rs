//! Transaction gossip control on the node (pool submit lives in IPC manager).

use node_subtensor_runtime::opaque::Block;
use sc_network::{PeerId, config::MultiaddrWithPeerId};
use sc_network_transactions::TransactionsHandlerController;
use sp_runtime::traits::Block as BlockT;

use crate::propagation_tracker::PropagationTracker;
use std::sync::Arc;

/// Handle to Substrate's transaction gossip handler.
#[derive(Clone)]
pub struct TxPropagator {
    controller: TransactionsHandlerController<<Block as BlockT>::Hash>,
    propagation_tracker: Option<Arc<PropagationTracker>>,
}

impl TxPropagator {
    pub fn new(
        controller: TransactionsHandlerController<<Block as BlockT>::Hash>,
        propagation_tracker: Option<Arc<PropagationTracker>>,
    ) -> Self {
        Self {
            controller,
            propagation_tracker,
        }
    }

    pub fn propagate(&self, hash: <Block as BlockT>::Hash) {
        let hash_str = format!("{hash:?}");
        if let Some(tracker) = &self.propagation_tracker {
            tracker.begin_own_propagation(hash_str.clone());
        }
        log::info!(
            target: "bot::transact",
            "📡 P2P propagate hash={hash_str}",
        );
        self.controller.propagate_transaction(hash);
    }

    pub fn propagate_all_ready(&self) {
        log::info!(
            target: "bot::transact",
            "📡 P2P propagate all ready pool txs",
        );
        self.controller.propagate_transactions();
    }
}

/// Parse a base58 peer id or a multiaddr ending in `/p2p/<peer_id>`.
pub fn parse_propagation_peer_id(raw: &str) -> Result<PeerId, String> {
    if let Ok(peer) = raw.parse::<MultiaddrWithPeerId>() {
        return Ok(peer.peer_id.into());
    }
    raw.parse::<PeerId>()
        .map_err(|e| format!("invalid peer id: {e}"))
}
