//! Tracks which connected peers are associated with early block announces.
//!
//! The public `BlockAnnounceValidator` API does not expose the announcing peer,
//! so we correlate each first-seen announce for block `N` with peers whose
//! reported `best_number` is already at or beyond `N`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// One row in the peer leaderboard.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerStat {
    pub peer_id: String,
    pub score: u64,
    pub first_announce_hits: u64,
    pub last_best_number: u32,
    pub roles: String,
}

/// A peer worth pinning via `--reserved-peers`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerRecommendation {
    pub peer_id: String,
    pub score: u64,
    /// Copy-paste hint. Requires an IP/DNS from `system_peers` or your logs.
    pub reserved_peer_hint: String,
}

#[derive(Clone, Debug)]
struct PeerRecord {
    score: u64,
    first_announce_hits: u64,
    last_best_number: u32,
    roles: String,
    last_seen_ms: u64,
}

#[derive(Default)]
pub struct PeerTracker {
    peers: RwLock<HashMap<String, PeerRecord>>,
    /// First attributed peer per block height (for debugging / RPC).
    first_by_block: RwLock<HashMap<u32, String>>,
}

impl PeerTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// `(peer_id_base58, best_number, roles_debug_string)`
    pub fn record_announce(
        &self,
        block_number: u32,
        peers: impl IntoIterator<Item = (String, u64, String)>,
    ) {
        let block_u64 = u64::from(block_number);
        let mut candidates: Vec<(String, u64, String)> = peers
            .into_iter()
            .filter(|(_, best, _)| *best >= block_u64)
            .collect();

        if candidates.is_empty() {
            log::debug!(
                target: "bot::peers",
                "block #{block_number}: no connected peer reported best_number >= block",
            );
            return;
        }

        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let winner = candidates[0].0.clone();
        {
            let mut first = self.first_by_block.write().expect("poisoned");
            first.entry(block_number).or_insert(winner.clone());
        }
        log::info!(
            target: "bot::peers",
            "block #{block_number} first announce attributed to {winner} ({} candidates)",
            candidates.len(),
        );

        let now_ms = now_ms();
        let mut peers = self.peers.write().expect("poisoned");

        for (peer_id, best, roles) in candidates {
            let entry = peers.entry(peer_id.clone()).or_insert(PeerRecord {
                score: 0,
                first_announce_hits: 0,
                last_best_number: block_number,
                roles: roles.clone(),
                last_seen_ms: now_ms,
            });

            entry.score = entry.score.saturating_add(1);
            if best == block_u64 {
                entry.score = entry.score.saturating_add(5);
            }
            if peer_id == winner {
                entry.first_announce_hits = entry.first_announce_hits.saturating_add(1);
            }
            entry.last_best_number = block_number;
            entry.roles = roles;
            entry.last_seen_ms = now_ms;
        }
    }

    pub fn top_peers(&self, limit: usize) -> Vec<PeerStat> {
        let peers = self.peers.read().expect("poisoned");
        let mut rows: Vec<PeerStat> = peers
            .iter()
            .map(|(peer_id, rec)| PeerStat {
                peer_id: peer_id.clone(),
                score: rec.score,
                first_announce_hits: rec.first_announce_hits,
                last_best_number: rec.last_best_number,
                roles: rec.roles.clone(),
            })
            .collect();

        rows.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| b.first_announce_hits.cmp(&a.first_announce_hits))
                .then_with(|| a.peer_id.cmp(&b.peer_id))
        });
        rows.truncate(limit);
        rows
    }

    pub fn recommendations(&self, limit: usize) -> Vec<PeerRecommendation> {
        self.top_peers(limit)
            .into_iter()
            .map(|row| PeerRecommendation {
                peer_id: row.peer_id.clone(),
                score: row.score,
                reserved_peer_hint: format!("/p2p/{}", row.peer_id),
            })
            .collect()
    }

    pub fn first_peer_for_block(&self, block_number: u32) -> Option<String> {
        self.first_by_block
            .read()
            .expect("poisoned")
            .get(&block_number)
            .cloned()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
