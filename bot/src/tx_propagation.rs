//! Runtime control for outbound transaction gossip strategy.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Toggle for reserved-node-first transaction propagation ordering.
pub struct TxPropagationControl {
    first_reserved_node: AtomicBool,
    /// Max outbound tx peers per propagation round; `0` = no send limit (all ranked peers).
    /// Does not affect incoming gossip — txs are accepted from every connected tx peer.
    max_propagation_peers: AtomicU32,
}

impl Default for TxPropagationControl {
    fn default() -> Self {
        Self {
            first_reserved_node: AtomicBool::new(false),
            max_propagation_peers: AtomicU32::new(0),
        }
    }
}

impl TxPropagationControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable_first_reserved_node(&self) {
        self.first_reserved_node.store(true, Ordering::SeqCst);
        log::info!(
            target: "bot::transact",
            "tx propagation: reserved nodes first, then others",
        );
    }

    pub fn disable_first_reserved_node(&self) {
        self.first_reserved_node.store(false, Ordering::SeqCst);
        log::info!(
            target: "bot::transact",
            "tx propagation: all peers by score (default)",
        );
    }

    pub fn first_reserved_node(&self) -> bool {
        self.first_reserved_node.load(Ordering::SeqCst)
    }

    /// Limit outbound tx gossip to the first `max` ranked peers. `0` removes the send limit.
    pub fn set_max_propagation_peers(&self, max: u32) {
        self.max_propagation_peers.store(max, Ordering::SeqCst);
        if max == 0 {
            log::info!(
                target: "bot::transact",
                "tx propagation: no outbound peer limit (send to all ranked peers)",
            );
        } else {
            log::info!(
                target: "bot::transact",
                "tx propagation: send to max {} ranked peer(s) per round (receive from all)",
                max,
            );
        }
    }

    /// Outbound send limit. `0` means send to all ranked peers.
    pub fn max_propagation_peers(&self) -> u32 {
        self.max_propagation_peers.load(Ordering::SeqCst)
    }
}
