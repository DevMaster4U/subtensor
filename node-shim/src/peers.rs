//! Tracks which connected peers are associated with early block announces
//! and successful outbound transaction gossip.
//!
//! When the patched sync engine passes the announcing peer id, we attribute
//! block `N` to that peer directly. Otherwise we fall back to correlating with
//! peers whose reported `best_number` is already at or beyond `N`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
struct PeerRecord {
    score: u64,
    first_announce_hits: u64,
    tx_propagation_hits: u64,
    last_best_number: u32,
    roles: String,
    last_seen_ms: u64,
    announce_time_total_ms: u64,
    announce_time_samples: u64,
}

fn combined_score(rec: &PeerRecord) -> u64 {
    rec.score.saturating_add(rec.tx_propagation_hits)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerTrackerInfo {
    pub announce_score: u64,
    pub first_announce_hits: u64,
    pub tx_propagation_hits: u64,
    pub last_best_number: u32,
    pub roles: Option<String>,
}

#[derive(Default)]
pub struct PeerTracker {
    peers: RwLock<HashMap<String, PeerRecord>>,
    /// First attributed peer per block height (for debugging / RPC).
    first_by_block: RwLock<HashMap<u32, String>>,
    /// Announce order per block height (first → last).
    announce_order_by_block: RwLock<HashMap<u32, Vec<String>>>,
}

impl PeerTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// `(peer_id_base58, best_number, roles_debug_string)`
    ///
    /// When `announcing_peer` is set (patched sync path), that peer is credited
    /// as the first announcer for this block height.
    pub fn record_announce(
        &self,
        block_number: u32,
        peers: impl IntoIterator<Item = (String, u64, String)>,
        announcing_peer: Option<&str>,
    ) {
        let block_u64 = u64::from(block_number);
        let mut candidates: Vec<(String, u64, String)> = peers
            .into_iter()
            .filter(|(_, best, _)| *best >= block_u64)
            .collect();

        let winner = if let Some(explicit) = announcing_peer {
            explicit.to_string()
        } else if candidates.is_empty() {
            log::debug!(
                target: "bot::peers",
                "block #{block_number}: no connected peer reported best_number >= block",
            );
            return;
        } else {
            candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            candidates[0].0.clone()
        };

        {
            let mut first = self.first_by_block.write().expect("poisoned");
            first.entry(block_number).or_insert(winner.clone());
        }
        if announcing_peer.is_some() {
            log::info!(
                target: "bot::peers",
                "block #{block_number} first announce from {winner} (exact peer id)",
            );
        } else {
            log::info!(
                target: "bot::peers",
                "block #{block_number} first announce attributed to {winner} ({} candidates, heuristic)",
                candidates.len(),
            );
        }

        let now_ms = now_ms();
        let mut peers = self.peers.write().expect("poisoned");

        if announcing_peer.is_some() && !candidates.iter().any(|(id, _, _)| id == &winner) {
            let entry = peers.entry(winner.clone()).or_insert(PeerRecord {
                score: 0,
                first_announce_hits: 0,
                tx_propagation_hits: 0,
                last_best_number: block_number,
                roles: String::new(),
                last_seen_ms: now_ms,
                announce_time_total_ms: 0,
                announce_time_samples: 0,
            });
            entry.score = entry.score.saturating_add(1);
            entry.first_announce_hits = entry.first_announce_hits.saturating_add(1);
            entry.last_best_number = block_number;
            entry.last_seen_ms = now_ms;
            return;
        }

        if candidates.is_empty() {
            return;
        }

        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        for (peer_id, best, roles) in candidates {
            let entry = peers.entry(peer_id.clone()).or_insert(PeerRecord {
                score: 0,
                first_announce_hits: 0,
                tx_propagation_hits: 0,
                last_best_number: block_number,
                roles: roles.clone(),
                last_seen_ms: now_ms,
                announce_time_total_ms: 0,
                announce_time_samples: 0,
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

    /// Record peers that successfully received an outbound transaction gossip round.
    pub fn record_tx_propagation(&self, peer_ids: impl IntoIterator<Item = String>) {
        let now_ms = now_ms();
        let mut peers = self.peers.write().expect("poisoned");
        for peer_id in peer_ids {
            let entry = peers.entry(peer_id).or_insert(PeerRecord {
                score: 0,
                first_announce_hits: 0,
                tx_propagation_hits: 0,
                last_best_number: 0,
                roles: String::new(),
                last_seen_ms: now_ms,
                announce_time_total_ms: 0,
                announce_time_samples: 0,
            });
            entry.tx_propagation_hits = entry.tx_propagation_hits.saturating_add(1);
            entry.last_seen_ms = now_ms;
        }
    }

    /// Record an announce attribution and append `peer_id` to the block order list.
    pub fn record_announce_peer(&self, block_number: u32, peer_id: &str, delay_time_ms: u64) {
        {
            let mut order = self.announce_order_by_block.write().expect("poisoned");
            let entry = order.entry(block_number).or_default();
            if !entry.iter().any(|id| id == peer_id) {
                entry.push(peer_id.to_string());
            }
        }

        let now_ms = now_ms();
        let mut peers = self.peers.write().expect("poisoned");
        let entry = peers.entry(peer_id.to_string()).or_insert(PeerRecord {
            score: 0,
            first_announce_hits: 0,
            tx_propagation_hits: 0,
            last_best_number: block_number,
            roles: String::new(),
            last_seen_ms: now_ms,
            announce_time_total_ms: 0,
            announce_time_samples: 0,
        });
        entry.announce_time_total_ms = entry.announce_time_total_ms.saturating_add(delay_time_ms);
        entry.announce_time_samples = entry.announce_time_samples.saturating_add(1);
        entry.last_seen_ms = now_ms;
    }

    /// Announcing peers for `block_number` in arrival order.
    pub fn announce_order_for_block(&self, block_number: u32) -> Vec<String> {
        self.announce_order_by_block
            .read()
            .expect("poisoned")
            .get(&block_number)
            .cloned()
            .unwrap_or_default()
    }

    /// Latest block height with any recorded announce order.
    pub fn latest_announce_block(&self) -> Option<u32> {
        self.announce_order_by_block
            .read()
            .expect("poisoned")
            .keys()
            .copied()
            .max()
    }

    pub fn first_peer_for_block(&self, block_number: u32) -> Option<String> {
        self.first_by_block
            .read()
            .expect("poisoned")
            .get(&block_number)
            .cloned()
    }

    /// Tracker fields for a peer id, if we have seen it on block announces or tx gossip.
    pub fn lookup(&self, peer_id: &str) -> Option<PeerTrackerInfo> {
        let peers = self.peers.read().expect("poisoned");
        peers.get(peer_id).map(|rec| PeerTrackerInfo {
            announce_score: combined_score(rec),
            first_announce_hits: rec.first_announce_hits,
            tx_propagation_hits: rec.tx_propagation_hits,
            last_best_number: rec.last_best_number,
            roles: if rec.roles.is_empty() {
                None
            } else {
                Some(rec.roles.clone())
            },
        })
    }

    /// Rank peer ids by combined score (announce + tx propagation), highest first.
    pub fn rank_peer_ids(&self, peer_ids: &[String]) -> Vec<String> {
        self.rank_peer_ids_by_function(peer_ids, "first_announce_hit_count")
    }

    /// Rank peers using `avg_announce_time` (lower average is better) or
    /// `first_announce_hit_count` (higher hits are better).
    pub fn rank_peer_ids_by_function(&self, peer_ids: &[String], rank_function: &str) -> Vec<String> {
        let scores = self.peers.read().expect("poisoned");
        let mut rows: Vec<(String, u64)> = peer_ids
            .iter()
            .map(|peer_id| {
                let key = match rank_function {
                    "avg_announce_time" => scores.get(peer_id).map(|rec| {
                        if rec.announce_time_samples == 0 {
                            u64::MAX
                        } else {
                            rec.announce_time_total_ms / rec.announce_time_samples
                        }
                    }),
                    "first_announce_hit_count" | _ => scores
                        .get(peer_id)
                        .map(|rec| u64::MAX - rec.first_announce_hits),
                };
                let rank_key = key.unwrap_or(u64::MAX);
                (peer_id.clone(), rank_key)
            })
            .collect();

        rows.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        rows.into_iter().map(|(peer_id, _)| peer_id).collect()
    }
}

/// Connected peer snapshot (litep2p leaves `network_state().connected_peers` empty).
#[derive(Clone, Debug, Default)]
pub struct ConnectedSnapshot {
    pub total: u32,
    pub sync_peers: HashSet<sc_network::PeerId>,
    /// Populated on libp2p backend; often empty on litep2p.
    pub addresses: HashMap<String, String>,
}

pub fn peer_is_connected(snapshot: &ConnectedSnapshot, peer_id: &sc_network::PeerId) -> bool {
    snapshot.sync_peers.contains(peer_id)
        || snapshot.addresses.contains_key(&peer_id.to_base58())
}

/// Build a connected-peer view using sync + network status (works with litep2p).
pub async fn connected_snapshot(
    sync: &sc_network_sync::SyncingService<node_subtensor_runtime::opaque::Block>,
    network: Arc<dyn sc_network::NetworkPeers + Send + Sync>,
    network_status: Arc<dyn sc_network::NetworkStatusProvider + Send + Sync>,
) -> ConnectedSnapshot {
    let sync_peers: HashSet<sc_network::PeerId> = sync
        .peers_info()
        .await
        .map(|peers| peers.into_iter().map(|(id, _)| id).collect())
        .unwrap_or_default();

    let addresses = connected_peer_addresses(network_status.as_ref()).await;

    let mut total = network_status
        .status()
        .await
        .map(|s| s.num_connected_peers)
        .unwrap_or_else(|_| network.sync_num_connected()) as u32;
    total = total.max(sync_peers.len() as u32);

    ConnectedSnapshot {
        total,
        sync_peers,
        addresses,
    }
}

/// Connected peer id → multiaddr from [`NetworkStatusProvider::network_state`].
pub async fn connected_peer_addresses(
    network: &(dyn sc_network::NetworkStatusProvider + Send + Sync),
) -> HashMap<String, String> {
    let Ok(state) = network.network_state().await else {
        return HashMap::new();
    };

    state
        .connected_peers
        .into_iter()
        .filter_map(|(peer_id, peer)| {
            peer.known_addresses
                .iter()
                .next()
                .map(|addr| (peer_id, addr.to_string()))
        })
        .collect()
}

pub(crate) fn is_dialable_multiaddr(multiaddr: &str) -> bool {
    multiaddr.contains("/ip4/")
        || multiaddr.contains("/ip6/")
        || multiaddr.contains("/dns/")
        || multiaddr.contains("/dns4/")
        || multiaddr.contains("/dns6/")
}

pub const DEFAULT_DISABLE_PEERS_FILE: &str = "config/disable_peers.txt";

/// Parse a peer-id list file (one base58 peer id per line).
pub fn parse_disable_peers_file(path: &str) -> Result<Vec<sc_network::PeerId>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {path}: {e}"))?;

    let mut peers = Vec::new();
    let mut errors = Vec::new();

    for (line_no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.parse::<sc_network::PeerId>() {
            Ok(peer) => peers.push(peer),
            Err(e) => errors.push(format!("line {}: {e}", line_no + 1)),
        }
    }

    if !errors.is_empty() {
        return Err(errors.join("; "));
    }

    Ok(peers)
}

/// Write peer ids to a disable list file (one per line).
pub fn write_disable_peers_file(path: &str, peer_ids: &[String]) -> Result<(), String> {
    let mut content = String::from("# Disabled peer ids (base58), one per line\n");
    for peer_id in peer_ids {
        content.push_str(peer_id);
        content.push('\n');
    }
    std::fs::write(path, content).map_err(|e| format!("failed to write {path}: {e}"))
}

/// Connection direction from libp2p network state (`out` = we dialed, `in` = they dialed us).
pub fn endpoint_direction(
    endpoint: &sc_network::network_state::PeerEndpoint,
) -> &'static str {
    use sc_network::network_state::PeerEndpoint;
    match endpoint {
        PeerEndpoint::Dialing(_, _) => "out",
        PeerEndpoint::Listening { .. } => "in",
    }
}

/// Connected peer id → connection direction (`in` / `out`) from network state (libp2p only).
pub async fn connected_peer_directions(
    network: &(dyn sc_network::NetworkStatusProvider + Send + Sync),
) -> HashMap<String, String> {
    let Ok(state) = network.network_state().await else {
        return HashMap::new();
    };

    state
        .connected_peers
        .into_iter()
        .map(|(peer_id, peer)| (peer_id, endpoint_direction(&peer.endpoint).into()))
        .collect()
}

/// Extra libp2p network-state fields per connected peer (empty on litep2p).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct NetworkPeerDetails {
    pub version: Option<String>,
    pub latest_ping_ms: Option<u64>,
    pub known_addresses: Vec<String>,
}

pub async fn connected_peer_details(
    network: &(dyn sc_network::NetworkStatusProvider + Send + Sync),
) -> HashMap<String, NetworkPeerDetails> {
    let Ok(state) = network.network_state().await else {
        return HashMap::new();
    };

    state
        .connected_peers
        .into_iter()
        .map(|(peer_id, peer)| {
            (
                peer_id,
                NetworkPeerDetails {
                    version: peer.version_string,
                    latest_ping_ms: peer.latest_ping_time.map(|d| d.as_millis() as u64),
                    known_addresses: peer
                        .known_addresses
                        .into_iter()
                        .map(|a| a.to_string())
                        .collect(),
                },
            )
        })
        .collect()
}

pub fn parse_reserved_peers_file(path: &str) -> Result<Vec<sc_network::config::MultiaddrWithPeerId>, String> {
    use sc_network::config::MultiaddrWithPeerId;

    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {path}: {e}"))?;

    let mut peers = Vec::new();
    let mut errors = Vec::new();

    for (line_no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.parse::<MultiaddrWithPeerId>() {
            Ok(peer) => peers.push(peer),
            Err(e) => errors.push(format!("line {}: {e}", line_no + 1)),
        }
    }

    if !errors.is_empty() {
        return Err(errors.join("; "));
    }

    Ok(peers)
}

/// Transaction gossip protocol name for the chain genesis.
pub fn transactions_protocol_name(genesis_hash: &[u8]) -> sc_network::ProtocolName {
    format!("/{}/transactions/1", hex::encode(genesis_hash)).into()
}

/// Block announce protocol name for the chain genesis (matches Substrate sync engine).
pub fn block_announces_protocol_name(genesis_hash: &[u8]) -> sc_network::ProtocolName {
    format!("/{}/block-announces/1", hex::encode(genesis_hash)).into()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
