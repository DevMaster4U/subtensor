//! Runtime control for outbound transaction gossip strategy.

use sc_network::PeerId;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// Outbound gossip strategy (see `bot_setPropagateMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum PropagateMode {
    /// Default ranking (`bot_propagateToPeers` allowlist, reserved-first, max peers).
    Normal = 0,
    /// Gossip only to the last block-announcing peer (first announce attribution).
    AnnounceFirst = 1,
    /// Gossip to all connected tx peers in parallel (no max-peer cap).
    Parallel = 2,
}

impl PropagateMode {
    pub fn from_u8(mode: u8) -> Option<Self> {
        match mode {
            0 => Some(Self::Normal),
            1 => Some(Self::AnnounceFirst),
            2 => Some(Self::Parallel),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::AnnounceFirst => "announce",
            Self::Parallel => "parallel",
        }
    }
}

/// Toggle for reserved-node-first transaction propagation ordering.
pub struct TxPropagationControl {
    first_reserved_node: AtomicBool,
    /// Max outbound tx peers per propagation round; `0` = no send limit (all ranked peers).
    /// Ignored while [`Self::propagation_allowlist`] is set or mode is [`PropagateMode::Parallel`].
    max_propagation_peers: AtomicU32,
    /// When set, outbound gossip goes only to these peers (in list order).
    propagation_allowlist: RwLock<Option<Vec<PeerId>>>,
    propagate_mode: AtomicU32,
}

impl Default for TxPropagationControl {
    fn default() -> Self {
        Self {
            first_reserved_node: AtomicBool::new(false),
            max_propagation_peers: AtomicU32::new(0),
            propagation_allowlist: RwLock::new(None),
            propagate_mode: AtomicU32::new(PropagateMode::Normal.as_u8() as u32),
        }
    }
}

/// Result of setting the propagation peer allowlist.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SetPropagationPeersResult {
    /// `false` when the allowlist was cleared (empty `peer_ids`).
    pub enabled: bool,
    pub peers: Vec<String>,
    pub invalid_peer_ids: Vec<String>,
}

impl TxPropagationControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
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

    /// Outbound send limit. `0` means send to all ranked peers (unless allowlist is active).
    pub fn max_propagation_peers(&self) -> u32 {
        self.max_propagation_peers.load(Ordering::SeqCst)
    }

    /// Restrict all outbound tx gossip to `peer_ids` only. Empty slice clears the allowlist.
    pub fn set_propagation_allowlist(
        &self,
        peer_ids: Vec<String>,
        parse_peer: impl Fn(&str) -> Result<PeerId, String>,
    ) -> SetPropagationPeersResult {
        if peer_ids.is_empty() {
            *self
                .propagation_allowlist
                .write()
                .expect("propagation allowlist lock poisoned") = None;
            log::info!(
                target: "bot::transact",
                "tx propagation: allowlist cleared (default ranking)",
            );
            return SetPropagationPeersResult {
                enabled: false,
                peers: Vec::new(),
                invalid_peer_ids: Vec::new(),
            };
        }

        let mut allowlist = Vec::new();
        let mut invalid_peer_ids = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for raw in peer_ids {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            match parse_peer(raw) {
                Ok(peer_id) if seen.insert(peer_id) => allowlist.push(peer_id),
                Ok(_) => {}
                Err(reason) => invalid_peer_ids.push(format!("{raw}: {reason}")),
            }
        }

        let peers: Vec<String> = allowlist.iter().map(|p| p.to_base58()).collect();

        if allowlist.is_empty() {
            *self
                .propagation_allowlist
                .write()
                .expect("propagation allowlist lock poisoned") = None;
            return SetPropagationPeersResult {
                enabled: false,
                peers: Vec::new(),
                invalid_peer_ids,
            };
        }

        *self
            .propagation_allowlist
            .write()
            .expect("propagation allowlist lock poisoned") = Some(allowlist);

        log::info!(
            target: "bot::transact",
            "tx propagation: allowlist only {:?} (no other outbound peers)",
            peers,
        );

        SetPropagationPeersResult {
            enabled: true,
            peers,
            invalid_peer_ids,
        }
    }

    /// Configured outbound-only peer allowlist (`None` = use normal ranking).
    pub fn propagation_allowlist(&self) -> Option<Vec<PeerId>> {
        self.propagation_allowlist
            .read()
            .expect("propagation allowlist lock poisoned")
            .clone()
    }

    pub fn propagation_allowlist_base58(&self) -> Option<Vec<String>> {
        self.propagation_allowlist().map(|peers| {
            peers
                .into_iter()
                .map(|p| p.to_base58())
                .collect()
        })
    }

    pub fn set_propagate_mode(&self, mode: PropagateMode) {
        self.propagate_mode
            .store(mode.as_u8() as u32, Ordering::SeqCst);
        log::info!(
            target: "bot::transact",
            "tx propagation: mode={} ({})",
            mode.as_u8(),
            mode.label(),
        );
    }

    pub fn propagate_mode(&self) -> PropagateMode {
        PropagateMode::from_u8(self.propagate_mode.load(Ordering::SeqCst) as u8)
            .unwrap_or(PropagateMode::Normal)
    }
}
