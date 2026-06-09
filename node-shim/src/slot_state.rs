//! Rolling per-slot block announce summaries (`block_number % 20`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Number of slot positions in the Aura cycle tracked on the node.
pub const SLOT_COUNT: u32 = 20;

/// Maximum peer summaries retained per slot (sorted by average delay).
pub const MAX_PEERS_PER_SLOT: usize = 10;

/// Aggregated announce stats for one peer within a slot position.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SlotPeerSummary {
    pub peer_id: String,
    pub avg_delay_time_ms: u64,
    pub min_delay_time_ms: u64,
    pub max_delay_time_ms: u64,
    pub announce_count: u64,
}

/// Summary snapshot for one slot position (`block_number % 20`).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SlotState {
    pub slot: u32,
    /// Peer with the most first announces observed for this slot position.
    pub first_announce_peer_id: Option<String>,
    /// Up to [`MAX_PEERS_PER_SLOT`] peers, ascending by `avg_delay_time_ms`.
    pub peers: Vec<SlotPeerSummary>,
}

/// RPC export: all slot positions.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SlotStateExport {
    pub slots: Vec<SlotState>,
}

#[derive(Clone, Debug, Default)]
struct PeerSlotStats {
    delay_total_ms: u64,
    delay_samples: u64,
    min_delay_time_ms: u64,
    max_delay_time_ms: u64,
    first_announce_count: u64,
}

#[derive(Default)]
struct SlotAccumulator {
    peers: HashMap<String, PeerSlotStats>,
}

#[derive(Default)]
struct Inner {
    slots: [SlotAccumulator; SLOT_COUNT as usize],
}

/// Tracks aggregated block announce data per slot position.
pub struct SlotStateStore {
    inner: RwLock<Inner>,
}

impl SlotStateStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner::default()),
        })
    }

    /// Record an immediate-next block announce.
    pub fn record_announce(
        &self,
        block_number: u32,
        peer_id: &str,
        delay_time_ms: u64,
        is_first: bool,
    ) {
        let slot_idx = (block_number % SLOT_COUNT) as usize;
        let mut inner = self.inner.write().expect("poisoned");
        let slot = &mut inner.slots[slot_idx];

        let entry = slot.peers.entry(peer_id.to_string()).or_default();
        if entry.delay_samples == 0 {
            entry.min_delay_time_ms = delay_time_ms;
            entry.max_delay_time_ms = delay_time_ms;
        } else {
            entry.min_delay_time_ms = entry.min_delay_time_ms.min(delay_time_ms);
            entry.max_delay_time_ms = entry.max_delay_time_ms.max(delay_time_ms);
        }
        entry.delay_total_ms = entry.delay_total_ms.saturating_add(delay_time_ms);
        entry.delay_samples = entry.delay_samples.saturating_add(1);
        if is_first {
            entry.first_announce_count = entry.first_announce_count.saturating_add(1);
        }
    }

    pub fn export(&self) -> SlotStateExport {
        let inner = self.inner.read().expect("poisoned");
        SlotStateExport {
            slots: (0..SLOT_COUNT)
                .map(|slot| Self::build_slot_summary(slot, &inner.slots[slot as usize]))
                .collect(),
        }
    }

    pub fn slot(&self, slot: u32) -> Option<SlotState> {
        if slot >= SLOT_COUNT {
            return None;
        }
        let inner = self.inner.read().expect("poisoned");
        Some(Self::build_slot_summary(slot, &inner.slots[slot as usize]))
    }

    fn build_slot_summary(slot: u32, acc: &SlotAccumulator) -> SlotState {
        let first_announce_peer_id = acc
            .peers
            .iter()
            .max_by_key(|(_, stats)| stats.first_announce_count)
            .filter(|(_, stats)| stats.first_announce_count > 0)
            .map(|(peer_id, _)| peer_id.clone());

        let mut peers: Vec<SlotPeerSummary> = acc
            .peers
            .iter()
            .filter(|(_, stats)| stats.delay_samples > 0)
            .map(|(peer_id, stats)| SlotPeerSummary {
                peer_id: peer_id.clone(),
                avg_delay_time_ms: stats.delay_total_ms / stats.delay_samples,
                min_delay_time_ms: stats.min_delay_time_ms,
                max_delay_time_ms: stats.max_delay_time_ms,
                announce_count: stats.delay_samples,
            })
            .collect();

        peers.sort_by_key(|entry| entry.avg_delay_time_ms);
        if peers.len() > MAX_PEERS_PER_SLOT {
            peers.truncate(MAX_PEERS_PER_SLOT);
        }

        SlotState {
            slot,
            first_announce_peer_id,
            peers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_number_is_block_mod_20() {
        let store = SlotStateStore::new();
        store.record_announce(8192, "peerA", 500, true);
        let slot = store.slot(12).unwrap();
        assert_eq!(slot.first_announce_peer_id.as_deref(), Some("peerA"));
        assert_eq!(slot.peers.len(), 1);
        assert_eq!(slot.peers[0].avg_delay_time_ms, 500);
    }

    #[test]
    fn peer_summary_tracks_avg_min_max() {
        let store = SlotStateStore::new();
        store.record_announce(100, "peerA", 100, false);
        store.record_announce(120, "peerA", 300, false);
        store.record_announce(140, "peerA", 200, false);

        let slot = store.slot(0).unwrap();
        let peer = slot.peers.iter().find(|p| p.peer_id == "peerA").unwrap();
        assert_eq!(peer.avg_delay_time_ms, 200);
        assert_eq!(peer.min_delay_time_ms, 100);
        assert_eq!(peer.max_delay_time_ms, 300);
        assert_eq!(peer.announce_count, 3);
    }

    #[test]
    fn peers_sorted_by_avg_delay_and_capped_at_ten() {
        let store = SlotStateStore::new();
        for (peer, delay) in [
            ("p1", 900),
            ("p2", 100),
            ("p3", 500),
            ("p4", 200),
            ("p5", 800),
            ("p6", 300),
            ("p7", 50),
            ("p8", 700),
            ("p9", 400),
            ("p10", 600),
            ("p11", 950),
            ("p12", 150),
        ] {
            store.record_announce(100, peer, delay, false);
        }
        let slot = store.slot(0).unwrap();
        assert_eq!(slot.peers.len(), MAX_PEERS_PER_SLOT);
        let avgs: Vec<u64> = slot.peers.iter().map(|e| e.avg_delay_time_ms).collect();
        assert_eq!(avgs, vec![50, 100, 150, 200, 300, 400, 500, 600, 700, 800]);
    }

    #[test]
    fn first_announce_peer_is_most_frequent() {
        let store = SlotStateStore::new();
        store.record_announce(8192, "peerA", 500, true);
        store.record_announce(8212, "peerB", 200, true);
        store.record_announce(8232, "peerA", 400, true);
        let slot = store.slot(12).unwrap();
        assert_eq!(slot.first_announce_peer_id.as_deref(), Some("peerA"));
    }
}
