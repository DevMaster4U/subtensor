//! Runtime control for outbound transaction gossip strategy.

use sc_network::{config::MultiaddrWithPeerId, PeerId};
use sc_network_transactions::config::PropagationStrategy;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Finney reserved full-node used for bootnode-first tx gossip when none are connected.
const HARDCODED_CANDIDATE: &str =
    "/ip4/167.235.247.220/tcp/30333/ws/p2p/12D3KooWRwbMb85RWnT8DSXSYMWQtuDwh4LJzndoRrTDotTR5gDC";

/// Toggle for bootnode-first transaction propagation.
pub struct TxPropagationControl {
    only_bootnode: AtomicBool,
}

impl Default for TxPropagationControl {
    fn default() -> Self {
        Self {
            only_bootnode: AtomicBool::new(false),
        }
    }
}

impl TxPropagationControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable_only_bootnode(&self) {
        self.only_bootnode.store(true, Ordering::SeqCst);
        log::info!(
            target: "bot::transact",
            "tx propagation: bootnode first, then others",
        );
    }

    pub fn disable_only_bootnode(&self) {
        self.only_bootnode.store(false, Ordering::SeqCst);
        log::info!(
            target: "bot::transact",
            "tx propagation: all peers (default)",
        );
    }

    pub fn only_bootnode(&self) -> bool {
        self.only_bootnode.load(Ordering::SeqCst)
    }
}

/// Reads [`TxPropagationControl`] and configured bootnode / reserved peer ids.
pub struct BotPropagationStrategy {
    control: Arc<TxPropagationControl>,
    bootnodes: HashSet<PeerId>,
    candidate_peers: Vec<PeerId>,
    candidate_multiaddrs: Vec<sc_network::Multiaddr>,
}

impl BotPropagationStrategy {
    pub fn new(
        control: Arc<TxPropagationControl>,
        bootnodes: impl IntoIterator<Item = PeerId>,
        bootnode_multiaddrs: impl IntoIterator<Item = sc_network::Multiaddr>,
        reserved_nodes: impl IntoIterator<Item = MultiaddrWithPeerId>,
    ) -> Self {
        let bootnodes: HashSet<PeerId> = bootnodes.into_iter().collect();
        let mut candidate_peers = Vec::new();
        let mut candidate_multiaddrs = Vec::new();
        let mut seen = HashSet::new();

        let mut push_node = |node: &MultiaddrWithPeerId| {
            let peer_id: PeerId = node.peer_id.into();
            if seen.insert(peer_id) {
                candidate_peers.push(peer_id);
                candidate_multiaddrs.push(node.concat());
            }
        };

        // CLI `--reserved-nodes` first (dialable IPs).
        for node in reserved_nodes {
            push_node(&node);
        }

        // Hardcoded finney reserved fallback.
        if let Ok(node) = HARDCODED_CANDIDATE.parse::<MultiaddrWithPeerId>() {
            push_node(&node);
        }

        // Chain / CLI bootnodes last.
        for peer_id in &bootnodes {
            if seen.insert(*peer_id) {
                candidate_peers.push(*peer_id);
            }
        }
        for addr in bootnode_multiaddrs {
            if !candidate_multiaddrs.iter().any(|existing| existing == &addr) {
                candidate_multiaddrs.push(addr);
            }
        }

        log::info!(
            target: "bot::transact",
            "tx propagation candidates: peers={:?} multiaddrs={}",
            candidate_peers
                .iter()
                .map(|id| id.to_base58())
                .collect::<Vec<_>>(),
            candidate_multiaddrs.len(),
        );

        Self {
            control,
            bootnodes,
            candidate_peers,
            candidate_multiaddrs,
        }
    }
}

impl PropagationStrategy for BotPropagationStrategy {
    fn bootnode_first(&self) -> bool {
        self.control.only_bootnode()
    }

    fn bootnode_peers(&self) -> HashSet<PeerId> {
        self.bootnodes.clone()
    }

    fn bootnode_multiaddrs(&self) -> Vec<sc_network::Multiaddr> {
        self.candidate_multiaddrs.clone()
    }

    fn candidate_peers(&self) -> Vec<PeerId> {
        self.candidate_peers.clone()
    }

    fn candidate_multiaddrs(&self) -> Vec<sc_network::Multiaddr> {
        self.candidate_multiaddrs.clone()
    }
}
