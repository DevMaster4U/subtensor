//! Per-peer racing metrics and composite score for trading peer selection.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const WEIGHT_FIRST_BLOCK: f64 = 0.6;
const WEIGHT_RTT: f64 = 0.3;
const WEIGHT_UPTIME: f64 = 0.1;

#[derive(Clone, Debug, Default)]
struct PeerStats {
    latest_rtt_ms: Option<u64>,
    rtt_total_ms: u64,
    rtt_samples: u64,
    blocks_received_first: u64,
    announce_delay_total_ms: u64,
    announce_delay_samples: u64,
    disconnect_count: u64,
    connect_count: u64,
    connected: bool,
    connected_since_ms: Option<u64>,
    total_connected_ms: u64,
}

#[derive(Default)]
struct Inner {
    /// Blocks where we observed at least one immediate-next announce.
    total_blocks: u64,
    last_block_number: Option<u32>,
    peers: HashMap<String, PeerStats>,
}

/// Tracks peer racing stats and exposes ranked scores via RPC.
pub struct PeerScoreboard {
    inner: RwLock<Inner>,
}

impl PeerScoreboard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner::default()),
        })
    }

    pub fn record_ping(&self, peer_id: &str, rtt_ms: u64) {
        let mut inner = self.inner.write().expect("poisoned");
        let entry = inner.peers.entry(peer_id.to_string()).or_default();
        entry.latest_rtt_ms = Some(rtt_ms);
        entry.rtt_total_ms = entry.rtt_total_ms.saturating_add(rtt_ms);
        entry.rtt_samples = entry.rtt_samples.saturating_add(1);
    }

    pub fn record_block_announce(&self, block_number: u32, peer_id: &str, delay_time_ms: u64, is_first: bool) {
        let mut inner = self.inner.write().expect("poisoned");
        if inner.last_block_number != Some(block_number) {
            inner.total_blocks = inner.total_blocks.saturating_add(1);
            inner.last_block_number = Some(block_number);
        }

        let entry = inner.peers.entry(peer_id.to_string()).or_default();
        entry.announce_delay_total_ms = entry.announce_delay_total_ms.saturating_add(delay_time_ms);
        entry.announce_delay_samples = entry.announce_delay_samples.saturating_add(1);
        if is_first {
            entry.blocks_received_first = entry.blocks_received_first.saturating_add(1);
        }
    }

    pub fn record_connect(&self, peer_id: &str) {
        let now = now_ms();
        let mut inner = self.inner.write().expect("poisoned");
        let entry = inner.peers.entry(peer_id.to_string()).or_default();
        if !entry.connected {
            entry.connect_count = entry.connect_count.saturating_add(1);
            entry.connected = true;
            entry.connected_since_ms = Some(now);
        }
    }

    pub fn record_disconnect(&self, peer_id: &str) {
        let now = now_ms();
        let mut inner = self.inner.write().expect("poisoned");
        let entry = inner.peers.entry(peer_id.to_string()).or_default();
        if entry.connected {
            if let Some(since) = entry.connected_since_ms.take() {
                entry.total_connected_ms = entry.total_connected_ms.saturating_add(now.saturating_sub(since));
            }
            entry.connected = false;
        }
        entry.disconnect_count = entry.disconnect_count.saturating_add(1);
    }

    /// Single peer score entry (same fields as [`PeerScoreboardExport::peers`]).
    pub fn entry_for(&self, peer_id: &str, connected: bool) -> PeerScoreEntry {
        let now = now_ms();
        let inner = self.inner.read().expect("poisoned");
        let total_blocks = inner.total_blocks.max(1);
        let stats = inner.peers.get(peer_id).cloned().unwrap_or_default();

        let avg_rtt_ms = if stats.rtt_samples == 0 {
            None
        } else {
            Some(stats.rtt_total_ms / stats.rtt_samples)
        };
        let avg_block_announcement_delay_ms = if stats.announce_delay_samples == 0 {
            None
        } else {
            Some(stats.announce_delay_total_ms / stats.announce_delay_samples)
        };

        let first_block_percentage = stats.blocks_received_first as f64 / total_blocks as f64;

        let rtt_values: Vec<u64> = inner
            .peers
            .values()
            .filter_map(|p| p.latest_rtt_ms)
            .collect();
        let min_rtt = rtt_values.into_iter().min();

        let rtt_score = match (stats.latest_rtt_ms, min_rtt) {
            (Some(rtt), Some(min)) if rtt > 0 => (min as f64 / rtt as f64).min(1.0),
            _ => 0.0,
        };

        let mut total_connected_ms = stats.total_connected_ms;
        if stats.connected {
            if let Some(since) = stats.connected_since_ms {
                total_connected_ms =
                    total_connected_ms.saturating_add(now.saturating_sub(since));
            }
        }
        let sessions = stats.connect_count + stats.disconnect_count;
        let uptime_score = if sessions == 0 && (stats.connected || connected) {
            1.0
        } else if sessions == 0 {
            0.0
        } else {
            let connected_fraction = stats.connect_count as f64 / sessions as f64;
            let duration_factor = if total_connected_ms > 0 { 1.0 } else { 0.5 };
            (connected_fraction * duration_factor).min(1.0)
        };

        let score = WEIGHT_FIRST_BLOCK * first_block_percentage
            + WEIGHT_RTT * rtt_score
            + WEIGHT_UPTIME * uptime_score;

        PeerScoreEntry {
            peer_id: peer_id.to_string(),
            connected: stats.connected || connected,
            rtt_ms: stats.latest_rtt_ms,
            avg_rtt_ms,
            blocks_received_first: stats.blocks_received_first,
            first_block_percentage,
            avg_block_announcement_delay_ms,
            disconnect_count: stats.disconnect_count,
            connect_count: stats.connect_count,
            uptime_score,
            rtt_score,
            score,
        }
    }

    /// Build ranked export, including connected peers with no stats yet.
    pub fn export_ranked(&self, connected_peer_ids: impl IntoIterator<Item = String>) -> PeerScoreboardExport {
        let now = now_ms();
        let inner = self.inner.read().expect("poisoned");
        let total_blocks = inner.total_blocks.max(1);

        let connected_set: HashSet<String> = connected_peer_ids.into_iter().collect();
        let mut peer_ids: HashSet<String> = inner.peers.keys().cloned().collect();
        for id in &connected_set {
            peer_ids.insert(id.clone());
        }

        let mut rtt_values: Vec<u64> = peer_ids
            .iter()
            .filter_map(|id| inner.peers.get(id).and_then(|p| p.latest_rtt_ms))
            .collect();
        rtt_values.sort_unstable();
        let min_rtt = rtt_values.first().copied();

        let mut entries: Vec<PeerScoreEntry> = peer_ids
            .into_iter()
            .map(|peer_id| {
                let stats = inner.peers.get(&peer_id).cloned().unwrap_or_default();
                let avg_rtt_ms = if stats.rtt_samples == 0 {
                    None
                } else {
                    Some(stats.rtt_total_ms / stats.rtt_samples)
                };
                let avg_block_announcement_delay_ms = if stats.announce_delay_samples == 0 {
                    None
                } else {
                    Some(stats.announce_delay_total_ms / stats.announce_delay_samples)
                };

                let first_block_percentage = stats.blocks_received_first as f64 / total_blocks as f64;

                let rtt_score = match (stats.latest_rtt_ms, min_rtt) {
                    (Some(rtt), Some(min)) if rtt > 0 => (min as f64 / rtt as f64).min(1.0),
                    _ => 0.0,
                };

                let mut total_connected_ms = stats.total_connected_ms;
                if stats.connected {
                    if let Some(since) = stats.connected_since_ms {
                        total_connected_ms =
                            total_connected_ms.saturating_add(now.saturating_sub(since));
                    }
                }
                let sessions = stats.connect_count + stats.disconnect_count;
                let uptime_score = if sessions == 0 && stats.connected {
                    1.0
                } else if sessions == 0 {
                    0.0
                } else {
                    let connected_fraction = stats.connect_count as f64 / sessions as f64;
                    let duration_factor = if total_connected_ms > 0 {
                        1.0
                    } else {
                        0.5
                    };
                    (connected_fraction * duration_factor).min(1.0)
                };

                let score = WEIGHT_FIRST_BLOCK * first_block_percentage
                    + WEIGHT_RTT * rtt_score
                    + WEIGHT_UPTIME * uptime_score;

                let connected = stats.connected || connected_set.contains(&peer_id);

                PeerScoreEntry {
                    peer_id,
                    connected,
                    rtt_ms: stats.latest_rtt_ms,
                    avg_rtt_ms,
                    blocks_received_first: stats.blocks_received_first,
                    first_block_percentage,
                    avg_block_announcement_delay_ms,
                    disconnect_count: stats.disconnect_count,
                    connect_count: stats.connect_count,
                    uptime_score,
                    rtt_score,
                    score,
                }
            })
            .collect();

        entries.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.peer_id.cmp(&b.peer_id))
        });

        PeerScoreboardExport {
            total_blocks: inner.total_blocks,
            peers: entries,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerScoreEntry {
    pub peer_id: String,
    pub connected: bool,
    pub rtt_ms: Option<u64>,
    pub avg_rtt_ms: Option<u64>,
    pub blocks_received_first: u64,
    pub first_block_percentage: f64,
    pub avg_block_announcement_delay_ms: Option<u64>,
    pub disconnect_count: u64,
    pub connect_count: u64,
    pub uptime_score: f64,
    pub rtt_score: f64,
    pub score: f64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerScoreboardExport {
    pub total_blocks: u64,
    pub peers: Vec<PeerScoreEntry>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
