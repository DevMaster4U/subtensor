//! Tx gossip ranking and propagation scoring hooks for `sc-network-transactions`.

use node_subtensor_runtime::opaque::Block;
use sc_network::{config::MultiaddrWithPeerId, PeerId};
use sc_network::NetworkStatusProvider;
use sc_network_transactions::config::{PeerRanker, PropagationObserver, PropagationReport};
use sp_runtime::traits::Block as BlockT;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::peers::PeerTracker;
use crate::propagation_tracker::PropagationTracker;
use crate::tx_propagation::{PropagateMode, RankFunction, TxPropagationControl};

/// Orders outbound tx gossip using [`TxPropagationControl`] and [`PeerTracker`].
pub struct BotPeerRanker {
    peer_tracker: Arc<PeerTracker>,
    control: Arc<TxPropagationControl>,
    propagation_tracker: Arc<PropagationTracker>,
    reserved_peer_ids: HashSet<PeerId>,
    reserved_multiaddrs: Vec<sc_network::Multiaddr>,
}

impl BotPeerRanker {
    pub fn new(
        peer_tracker: Arc<PeerTracker>,
        control: Arc<TxPropagationControl>,
        propagation_tracker: Arc<PropagationTracker>,
        reserved_nodes: impl IntoIterator<Item = MultiaddrWithPeerId>,
    ) -> Self {
        let mut reserved_peer_ids = HashSet::new();
        let mut reserved_multiaddrs = Vec::new();
        for node in reserved_nodes {
            let peer_id: PeerId = node.peer_id.into();
            if reserved_peer_ids.insert(peer_id) {
                reserved_multiaddrs.push(node.concat());
            }
        }

        Self {
            peer_tracker,
            control,
            propagation_tracker,
            reserved_peer_ids,
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

    fn rank_by_tracker(&self, peers: &[PeerId], rank_function: RankFunction) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        let ids: Vec<String> = peers.iter().map(|p| p.to_base58()).collect();
        let ranked_ids = self
            .peer_tracker
            .rank_peer_ids_by_function(&ids, rank_function.as_str());
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

    fn announce_peers(&self, count: u32) -> Vec<PeerId> {
        let block = self
            .propagation_tracker
            .latest()
            .and_then(|r| r.last_block_number)
            .or_else(|| self.peer_tracker.latest_announce_block());

        let Some(block) = block else {
            return Vec::new();
        };

        let order = self.peer_tracker.announce_order_for_block(block);
        let take = if count == 0 {
            order.len()
        } else {
            count.min(order.len() as u32) as usize
        };

        order
            .into_iter()
            .take(take)
            .filter_map(|id| id.parse().ok())
            .collect()
    }
}

impl PeerRanker for BotPeerRanker {
    fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        if peers.is_empty() {
            return Vec::new();
        }

        let mode = self.control.propagate_mode();

        if let Some(allowlist) = self.control.propagation_allowlist() {
            return self.rank_allowlist(peers, allowlist);
        }

        match mode {
            PropagateMode::AnnounceFirst => {
                if let Some(count) = self.control.pending_announce_count() {
                    return self.rank_allowlist(peers, self.announce_peers(count));
                }
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
                if let Some(count) = self.control.pending_announce_count() {
                    return self.rank_allowlist(peers, self.announce_peers(count));
                }
                peers.to_vec()
            }
            PropagateMode::Normal => {
                if let Some(count) = self.control.pending_announce_count() {
                    return self.rank_allowlist(peers, self.announce_peers(count));
                }

                let rank_function = self
                    .control
                    .pending_rank_function()
                    .unwrap_or(RankFunction::FirstAnnounceHitCount);

                if !self.control.first_reserved_node() {
                    return self.rank_by_tracker(peers, rank_function);
                }

                let connected: HashSet<PeerId> = peers.iter().copied().collect();
                let mut ranked = Vec::with_capacity(peers.len());
                let mut seen = HashSet::new();

                for reserved in &self.reserved_peer_ids {
                    if connected.contains(reserved) && seen.insert(*reserved) {
                        ranked.push(*reserved);
                    }
                }

                let rest: Vec<PeerId> = peers
                    .iter()
                    .copied()
                    .filter(|peer| seen.insert(*peer))
                    .collect();
                ranked.extend(self.rank_by_tracker(&rest, rank_function));
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
                } else if self.control.pending_announce_count().is_some() {
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
    tx_control: Arc<TxPropagationControl>,
}

impl BotPropagationObserver {
    pub fn new(
        peer_tracker: Arc<PeerTracker>,
        propagation_tracker: Arc<PropagationTracker>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
        tx_control: Arc<TxPropagationControl>,
    ) -> Self {
        Self {
            peer_tracker,
            propagation_tracker,
            network,
            tx_control,
        }
    }

    fn peer_addrs(&self) -> HashMap<String, String> {
        let network = Arc::clone(&self.network);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                crate::peers::connected_peer_addresses(network.as_ref()).await
            })
        })
    }
}

impl PropagationObserver<<Block as BlockT>::Hash> for BotPropagationObserver {
    fn on_propagated(&self, report: PropagationReport<<Block as BlockT>::Hash>) {
        let _ = self.tx_control.take_pending_request();

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
