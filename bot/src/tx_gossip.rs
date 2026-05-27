//! Tx gossip ranking and propagation scoring hooks for `sc-network-transactions`.

use crate::{
    peers::PeerTracker,
    tx_propagation::TxPropagationControl,
};
use node_subtensor_runtime::opaque::Block;
use sc_network::{config::MultiaddrWithPeerId, PeerId};
use sc_network_transactions::config::{PeerRanker, PropagationObserver};
use sp_runtime::traits::Block as BlockT;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Orders outbound tx gossip: `--reserved-nodes` first when enabled, else by score.
pub struct BotPeerRanker {
    peer_tracker: Arc<PeerTracker>,
    control: Arc<TxPropagationControl>,
    reserved_peers: Vec<PeerId>,
    reserved_multiaddrs: Vec<sc_network::Multiaddr>,
}

impl BotPeerRanker {
    pub fn new(
        peer_tracker: Arc<PeerTracker>,
        control: Arc<TxPropagationControl>,
        reserved_nodes: impl IntoIterator<Item = MultiaddrWithPeerId>,
    ) -> Self {
        let mut reserved_peers = Vec::new();
        let mut reserved_multiaddrs = Vec::new();
        let mut seen = HashSet::new();
        for node in reserved_nodes {
            let peer_id: PeerId = node.peer_id.into();
            if seen.insert(peer_id) {
                reserved_peers.push(peer_id);
                reserved_multiaddrs.push(node.concat());
            }
        }

        log::info!(
            target: "bot::transact",
            "tx propagation reserved peers: {:?}",
            reserved_peers
                .iter()
                .map(|id| id.to_base58())
                .collect::<Vec<_>>(),
        );

        Self {
            peer_tracker,
            control,
            reserved_peers,
            reserved_multiaddrs,
        }
    }

    fn rank_by_tracker(&self, peers: &[PeerId]) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        let ids: Vec<String> = peers.iter().map(|p| p.to_base58()).collect();
        let ranked_ids = self.peer_tracker.rank_peer_ids(&ids);
        let tracker_order: HashMap<&str, usize> = ranked_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        let mut sorted = peers.to_vec();
        sorted.sort_by_key(|peer| {
            tracker_order
                .get(peer.to_base58().as_str())
                .copied()
                .unwrap_or(usize::MAX)
        });
        sorted
    }
}

impl PeerRanker for BotPeerRanker {
    fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        if !self.control.first_reserved_node() {
            return self.rank_by_tracker(peers);
        }

        let connected: HashSet<PeerId> = peers.iter().copied().collect();
        let mut ranked = Vec::with_capacity(peers.len());
        let mut seen = HashSet::new();

        for reserved in &self.reserved_peers {
            if connected.contains(reserved) && seen.insert(*reserved) {
                ranked.push(*reserved);
            }
        }

        let rest: Vec<PeerId> = peers
            .iter()
            .copied()
            .filter(|peer| seen.insert(*peer))
            .collect();
        ranked.extend(self.rank_by_tracker(&rest));
        ranked
    }

    fn priority_multiaddrs(&self) -> Vec<sc_network::Multiaddr> {
        if self.control.first_reserved_node() {
            self.reserved_multiaddrs.clone()
        } else {
            Vec::new()
        }
    }

    fn max_propagation_peers(&self) -> u32 {
        self.control.max_propagation_peers()
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
            // log::info!(
            //     target: "bot::transact",
            //     "📡 P2P propagated hash={:?} dest_peers={:?}",
            //     hash,
            //     peers,
            // );
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
