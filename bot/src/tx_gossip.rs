//! Tx gossip ranking and propagation scoring hooks for `sc-network-transactions`.

use crate::peers::PeerTracker;
use node_subtensor_runtime::opaque::Block;
use sc_network::PeerId;
use sc_network_transactions::config::{PeerRanker, PropagationObserver};
use sp_runtime::traits::Block as BlockT;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Orders outbound tx gossip using [`PeerTracker`] combined scores.
pub struct BotPeerRanker {
    peer_tracker: Arc<PeerTracker>,
}

impl BotPeerRanker {
    pub fn new(peer_tracker: Arc<PeerTracker>) -> Self {
        Self { peer_tracker }
    }
}

impl PeerRanker for BotPeerRanker {
    fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        let ids: Vec<String> = peers.iter().map(|p| p.to_base58()).collect();
        let ranked = self.peer_tracker.rank_peer_ids(&ids);
        let order: HashMap<&str, usize> = ranked
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        let mut sorted = peers.to_vec();
        sorted.sort_by_key(|peer| {
            order
                .get(peer.to_base58().as_str())
                .copied()
                .unwrap_or(usize::MAX)
        });
        sorted
    }
}

/// Records which peers received each bot-initiated propagation round.
pub struct BotPropagationObserver {
    peer_tracker: Arc<PeerTracker>,
}

impl BotPropagationObserver {
    pub fn new(peer_tracker: Arc<PeerTracker>) -> Self {
        Self { peer_tracker }
    }
}

impl PropagationObserver<<Block as BlockT>::Hash> for BotPropagationObserver {
    fn on_propagated(&self, propagated: HashMap<<Block as BlockT>::Hash, Vec<String>>) {
        let mut unique = HashSet::new();
        for (hash, peers) in &propagated {
            log::info!(
                target: "bot::transact",
                "📡 P2P propagated hash={:?} dest_peers={:?}",
                hash,
                peers,
            );
            for peer_id in peers {
                unique.insert(peer_id.clone());
            }
        }
        if unique.is_empty() {
            return;
        }
        self.peer_tracker.record_tx_propagation(unique);
    }
}
