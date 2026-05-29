//! Tx gossip ranking and propagation scoring hooks for `sc-network-transactions`.

use crate::{
    peers::PeerTracker,
    propagation_tracker::PropagationTracker,
    reserved::ReservedPeerRegistry,
    tx_propagation::{PropagateMode, TxPropagationControl},
};
use node_subtensor_runtime::opaque::Block;
use sc_network::{config::MultiaddrWithPeerId, PeerId};
use sc_network::NetworkStatusProvider;
use sc_network_transactions::config::{PeerRanker, PropagationObserver, PropagationReport};
use sp_runtime::traits::Block as BlockT;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// One row in the current tx-gossip peer ranking (same order as outbound propagation).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RankedPeerRow {
    /// 1-based position in the ranked list.
    pub rank: u32,
    pub peer_id: String,
    pub score: u64,
    /// Connection multiaddr when known.
    pub addr: Option<String>,
    /// Present in `--reserved-nodes` (listed first when first-reserved mode is on).
    pub reserved: bool,
}

/// Orders outbound tx gossip: `--reserved-nodes` first when enabled, else by score.
pub struct BotPeerRanker {
    peer_tracker: Arc<PeerTracker>,
    control: Arc<TxPropagationControl>,
    propagation_tracker: Arc<PropagationTracker>,
    reserved_registry: Arc<ReservedPeerRegistry>,
    /// Startup `--reserved-nodes` multiaddrs (used for priority dial hints).
    reserved_multiaddrs: Vec<sc_network::Multiaddr>,
}

impl BotPeerRanker {
    pub fn new(
        peer_tracker: Arc<PeerTracker>,
        control: Arc<TxPropagationControl>,
        propagation_tracker: Arc<PropagationTracker>,
        reserved_registry: Arc<ReservedPeerRegistry>,
        reserved_nodes: impl IntoIterator<Item = MultiaddrWithPeerId>,
    ) -> Self {
        let mut reserved_multiaddrs = Vec::new();
        let mut seen = HashSet::new();
        for node in reserved_nodes {
            let peer_id: PeerId = node.peer_id.into();
            if seen.insert(peer_id) {
                reserved_registry.add_peer(peer_id);
                reserved_multiaddrs.push(node.concat());
            }
        }

        let reserved_peers = reserved_registry.peer_ids();
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
            propagation_tracker,
            reserved_registry,
            reserved_multiaddrs,
        }
    }

    fn rank_allowlist(&self, peers: &[PeerId], allowlist: Vec<PeerId>) -> Vec<PeerId> {
        let connected: HashSet<PeerId> = peers.iter().copied().collect();
        allowlist
            .into_iter()
            .filter(|peer| connected.contains(peer))
            .collect()
    }

    fn reserved_peer_ids(&self) -> Vec<PeerId> {
        self.reserved_registry.peer_ids().into_iter().collect()
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

    /// Rank connected peers (or all reserved peers when reserved-only mode is on).
    pub fn ranked_peer_list(
        &self,
        connected_peer_ids: &[String],
        addrs: &HashMap<String, String>,
    ) -> Vec<RankedPeerRow> {
        let reserved: HashSet<PeerId> = self.reserved_registry.peer_ids();

        let peer_ids: Vec<PeerId> = if self.reserved_registry.is_reserved_only() {
            reserved.iter().copied().collect()
        } else {
            connected_peer_ids
                .iter()
                .filter_map(|id| id.parse().ok())
                .collect()
        };

        if peer_ids.is_empty() {
            return Vec::new();
        }

        let ranked = self.rank_peers(&peer_ids);

        ranked
            .into_iter()
            .enumerate()
            .map(|(i, peer)| {
                let peer_id = peer.to_base58();
                let score = self
                    .peer_tracker
                    .lookup(&peer_id)
                    .map(|(combined, _, _, _, _)| combined)
                    .unwrap_or(0);
                RankedPeerRow {
                    rank: (i + 1) as u32,
                    peer_id: peer_id.clone(),
                    score,
                    addr: addrs.get(&peer_id).cloned(),
                    reserved: reserved.contains(&peer),
                }
            })
            .collect()
    }
}

impl PeerRanker for BotPeerRanker {
    fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        match self.control.propagate_mode() {
            PropagateMode::AnnounceFirst => {
                let Some(announcer) = self.propagation_tracker.last_announcing_peer_id() else {
                    return Vec::new();
                };
                let Ok(peer_id) = announcer.parse::<PeerId>() else {
                    return Vec::new();
                };
                let connected: HashSet<PeerId> = peers.iter().copied().collect();
                if connected.contains(&peer_id) {
                    vec![peer_id]
                } else {
                    Vec::new()
                }
            }
            PropagateMode::Parallel => {
                if let Some(allowlist) = self.control.propagation_allowlist() {
                    return self.rank_allowlist(peers, allowlist);
                }
                if self.reserved_registry.is_reserved_only() {
                    let reserved: HashSet<PeerId> = self.reserved_registry.peer_ids();
                    let subset: Vec<PeerId> = peers
                        .iter()
                        .copied()
                        .filter(|p| reserved.contains(p))
                        .collect();
                    return self.rank_by_tracker(&subset);
                }
                self.rank_by_tracker(peers)
            }
            PropagateMode::Normal => {
                if let Some(allowlist) = self.control.propagation_allowlist() {
                    return self.rank_allowlist(peers, allowlist);
                }

                if !self.control.first_reserved_node() {
                    return self.rank_by_tracker(peers);
                }

        let connected: HashSet<PeerId> = peers.iter().copied().collect();
        let mut ranked = Vec::with_capacity(peers.len());
        let mut seen = HashSet::new();

        for reserved in self.reserved_peer_ids() {
            if connected.contains(&reserved) && seen.insert(reserved) {
                ranked.push(reserved);
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
        }
    }

    fn priority_multiaddrs(&self) -> Vec<sc_network::Multiaddr> {
        if self.control.first_reserved_node() {
            self.reserved_multiaddrs.clone()
        } else {
            Vec::new()
        }
    }

    fn max_propagation_peers(&self) -> u32 {
        match self.control.propagate_mode() {
            PropagateMode::AnnounceFirst => 1,
            PropagateMode::Parallel => 0,
            PropagateMode::Normal => {
                if self.control.propagation_allowlist().is_some() {
                    0
                } else {
                    self.control.max_propagation_peers()
                }
            }
        }
    }
}

/// Records bot-initiated propagation rounds and updates peer tx scores.
pub struct BotPropagationObserver {
    peer_tracker: Arc<PeerTracker>,
    propagation_tracker: Arc<PropagationTracker>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
}

impl BotPropagationObserver {
    pub fn new(
        peer_tracker: Arc<PeerTracker>,
        propagation_tracker: Arc<PropagationTracker>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    ) -> Self {
        Self {
            peer_tracker,
            propagation_tracker,
            network,
        }
    }

    fn peer_addrs(&self) -> HashMap<String, String> {
        let network = Arc::clone(&self.network);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                network.connected_peer_addresses().await
            })
        })
    }
}

impl PropagationObserver<<Block as BlockT>::Hash> for BotPropagationObserver {
    fn on_propagated(&self, report: PropagationReport<<Block as BlockT>::Hash>) {
        let Some(pending_hash) = self.propagation_tracker.pending_own_tx_hash() else {
            return;
        };

        if !report.send_order.is_empty() {
            self.peer_tracker
                .record_tx_propagation(report.send_order.iter().cloned());
        }

        let addrs = self.peer_addrs();
        self.propagation_tracker.complete_own_propagation(
            &pending_hash,
            report.elapsed_ms,
            &report.send_order,
            &addrs,
        );
    }
}
