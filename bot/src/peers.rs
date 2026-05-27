//! Tracks which connected peers are associated with early block announces
//! and successful outbound transaction gossip.
//!
//! When the patched sync engine passes the announcing peer id, we attribute
//! block `N` to that peer directly. Otherwise we fall back to correlating with
//! peers whose reported `best_number` is already at or beyond `N`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// One row in the peer leaderboard.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerStat {
    pub peer_id: String,
    pub score: u64,
    pub first_announce_hits: u64,
    pub tx_propagation_hits: u64,
    pub last_best_number: u32,
    pub roles: String,
    /// Connection multiaddr when known (from network state / PEER_MAP).
    pub addr: Option<String>,
}

/// One peer row in a tx-gossip health check.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxGossipPeerRow {
    pub peer_id: String,
    pub announce_score: u64,
    pub tx_propagation_hits: u64,
    pub combined_score: u64,
    pub addr: Option<String>,
}

/// Summary returned by [`PeerTracker::tx_gossip_check`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxGossipCheck {
    /// Always `true` on builds with tx propagation scoring wired up.
    pub tx_propagation_scoring: bool,
    pub tracked_peers: u32,
    pub peers_with_tx_hits: u32,
    pub total_tx_propagation_hits: u64,
    pub total_announce_score: u64,
    pub top_tx_peers: Vec<TxGossipPeerRow>,
}

/// A peer worth pinning via `--reserved-peers`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerRecommendation {
    pub peer_id: String,
    pub score: u64,
    /// Full multiaddr for `--reserved-peers` when known, else `/p2p/<peer_id>`.
    pub reserved_peer_hint: String,
    /// Same as [`Self::reserved_peer_hint`] when a dialable address is known.
    pub addr: Option<String>,
}

#[derive(Clone, Debug)]
struct PeerRecord {
    score: u64,
    first_announce_hits: u64,
    tx_propagation_hits: u64,
    last_best_number: u32,
    roles: String,
    last_seen_ms: u64,
}

fn combined_score(rec: &PeerRecord) -> u64 {
    rec.score.saturating_add(rec.tx_propagation_hits)
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
            });
            entry.tx_propagation_hits = entry.tx_propagation_hits.saturating_add(1);
            entry.last_seen_ms = now_ms;
        }
    }

    pub fn top_peers(&self, limit: usize, addrs: Option<&HashMap<String, String>>) -> Vec<PeerStat> {
        let peers = self.peers.read().expect("poisoned");
        let mut rows: Vec<PeerStat> = peers
            .iter()
            .map(|(peer_id, rec)| PeerStat {
                peer_id: peer_id.clone(),
                score: combined_score(rec),
                first_announce_hits: rec.first_announce_hits,
                tx_propagation_hits: rec.tx_propagation_hits,
                last_best_number: rec.last_best_number,
                roles: rec.roles.clone(),
                addr: addrs.and_then(|m| m.get(peer_id).cloned()),
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

    /// Diagnostic summary for tx propagation scoring.
    pub fn tx_gossip_check(
        &self,
        top: usize,
        addrs: Option<&HashMap<String, String>>,
    ) -> TxGossipCheck {
        let peers = self.peers.read().expect("poisoned");
        let mut total_tx_propagation_hits = 0u64;
        let mut total_announce_score = 0u64;
        let mut peers_with_tx_hits = 0u32;
        let mut tx_rows: Vec<TxGossipPeerRow> = Vec::new();

        for (peer_id, rec) in peers.iter() {
            total_tx_propagation_hits =
                total_tx_propagation_hits.saturating_add(rec.tx_propagation_hits);
            total_announce_score = total_announce_score.saturating_add(rec.score);
            if rec.tx_propagation_hits > 0 {
                peers_with_tx_hits = peers_with_tx_hits.saturating_add(1);
                tx_rows.push(TxGossipPeerRow {
                    peer_id: peer_id.clone(),
                    announce_score: rec.score,
                    tx_propagation_hits: rec.tx_propagation_hits,
                    combined_score: combined_score(rec),
                    addr: addrs.and_then(|m| m.get(peer_id).cloned()),
                });
            }
        }

        tx_rows.sort_by(|a, b| {
            b.tx_propagation_hits
                .cmp(&a.tx_propagation_hits)
                .then_with(|| b.combined_score.cmp(&a.combined_score))
                .then_with(|| a.peer_id.cmp(&b.peer_id))
        });
        tx_rows.truncate(top);

        TxGossipCheck {
            tx_propagation_scoring: true,
            tracked_peers: peers.len() as u32,
            peers_with_tx_hits,
            total_tx_propagation_hits,
            total_announce_score,
            top_tx_peers: tx_rows,
        }
    }

    pub fn recommendations(
        &self,
        limit: usize,
        addrs: Option<&HashMap<String, String>>,
    ) -> Vec<PeerRecommendation> {
        self.top_peers(limit, addrs)
            .into_iter()
            .map(|row| {
                let hint = row
                    .addr
                    .clone()
                    .unwrap_or_else(|| format!("/p2p/{}", row.peer_id));
                PeerRecommendation {
                    peer_id: row.peer_id.clone(),
                    score: row.score,
                    reserved_peer_hint: hint.clone(),
                    addr: row.addr,
                }
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

    /// Tracker fields for a peer id, if we have seen it on block announces or tx gossip.
    pub fn lookup(&self, peer_id: &str) -> Option<(u64, u64, u64, u32, String)> {
        let peers = self.peers.read().expect("poisoned");
        peers.get(peer_id).map(|rec| {
            (
                combined_score(rec),
                rec.first_announce_hits,
                rec.tx_propagation_hits,
                rec.last_best_number,
                rec.roles.clone(),
            )
        })
    }

    /// Rank peer ids by combined score (announce + tx propagation), highest first.
    pub fn rank_peer_ids(&self, peer_ids: &[String]) -> Vec<String> {
        let scores = self.peers.read().expect("poisoned");
        let mut rows: Vec<(String, u64)> = peer_ids
            .iter()
            .map(|peer_id| {
                let score = scores
                    .get(peer_id)
                    .map(combined_score)
                    .unwrap_or(0);
                (peer_id.clone(), score)
            })
            .collect();

        rows.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.cmp(&b.0))
        });

        rows.into_iter().map(|(peer_id, _)| peer_id).collect()
    }

    /// Rank connected peers: combined score (desc), then chain best height (desc).
    pub fn rank_connected(&self, connected: &[(String, u64)]) -> Vec<String> {
        let scores = self.peers.read().expect("poisoned");
        let mut rows: Vec<(String, u64, u64)> = connected
            .iter()
            .map(|(peer_id, best_number)| {
                let score = scores
                    .get(peer_id)
                    .map(combined_score)
                    .unwrap_or(0);
                (peer_id.clone(), score, *best_number)
            })
            .collect();

        rows.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        rows.into_iter().map(|(peer_id, _, _)| peer_id).collect()
    }
}

// ── Peer pruning ──────────────────────────────────────────────────────────────

/// Full peer row written to filter logs.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FilterPeerDetail {
    pub peer_id: String,
    pub addr: Option<String>,
    pub roles: String,
    pub best_hash: String,
    pub best_number: u64,
    pub score: u64,
    pub first_announce_hits: u64,
    pub tx_propagation_hits: u64,
    pub last_best_number: u32,
    pub tracker_roles: Option<String>,
}

/// One filter run written under `filter_log/`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FilterLogEntry {
    pub timestamp: String,
    pub trigger: String,
    pub interval_secs: Option<u64>,
    pub keep_count: u32,
    pub connected_before: u32,
    pub kept_count: u32,
    pub dropped_count: u32,
    pub kept: Vec<FilterPeerDetail>,
    pub dropped: Vec<FilterPeerDetail>,
}

/// Result of [`PeerPruner::keep_top`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KeepTopPeersResult {
    pub connected_before: u32,
    pub kept_count: u32,
    pub dropped_count: u32,
    pub kept: Vec<String>,
    pub dropped: Vec<String>,
}

/// Result of [`PeerPruner::set_reserved_from_file`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SetReservedPeersResult {
    pub removed_count: u32,
    pub added_count: u32,
    /// Multiaddrs loaded from the file (deduplicated by peer id).
    pub peers: Vec<String>,
    /// Base58 peer ids loaded from the file.
    pub peer_ids: Vec<String>,
}

/// Disconnects connected peers outside the top-N by combined announce + tx score.
pub struct PeerPruner {
    sync: Arc<sc_network_sync::SyncingService<node_subtensor_runtime::opaque::Block>>,
    network: Arc<dyn sc_network::NetworkPeers + Send + Sync>,
    network_status: Arc<dyn sc_network::NetworkStatusProvider + Send + Sync>,
    block_announces_protocol: sc_network::ProtocolName,
    peer_tracker: Arc<PeerTracker>,
}

impl PeerPruner {
    pub fn new(
        sync: Arc<sc_network_sync::SyncingService<node_subtensor_runtime::opaque::Block>>,
        network: Arc<dyn sc_network::NetworkPeers + Send + Sync>,
        network_status: Arc<dyn sc_network::NetworkStatusProvider + Send + Sync>,
        block_announces_protocol: sc_network::ProtocolName,
        peer_tracker: Arc<PeerTracker>,
    ) -> Self {
        Self {
            sync,
            network,
            network_status,
            block_announces_protocol,
            peer_tracker,
        }
    }

    /// Keep the top `keep_count` connected peers (by [`PeerTracker`] score) and disconnect the rest.
    pub async fn keep_top(&self, keep_count: u32) -> Result<KeepTopPeersResult, String> {
        self.keep_top_with_log(keep_count, "rpc", None).await
    }

    /// Connected and previously seen peers with multiaddrs.
    pub async fn network_peers(&self) -> Result<Vec<NetworkPeerRow>, String> {
        collect_network_peers(
            self.network_status.as_ref(),
            self.sync.as_ref(),
            &self.peer_tracker,
        )
        .await
    }

    async fn keep_top_with_log(
        &self,
        keep_count: u32,
        trigger: &str,
        interval_secs: Option<u64>,
    ) -> Result<KeepTopPeersResult, String> {
        use sc_network::{PeerId, ReputationChange};
        use sp_runtime::traits::SaturatedConversion;

        let keep_count_usize = keep_count.clamp(1, 500) as usize;

        let connected = self
            .sync
            .peers_info()
            .await
            .map_err(|_| "sync engine unavailable".to_string())?;

        let addrs = self
            .network_status
            .connected_peer_addresses()
            .await;

        let mut details_by_id: HashMap<String, FilterPeerDetail> = HashMap::new();
        let mut connected_rows: Vec<(PeerId, String, u64)> = Vec::with_capacity(connected.len());

        for (peer_id, info) in connected {
            let id = peer_id.to_base58();
            let best: u64 = info.best_number.saturated_into();
            let (score, first_announce_hits, tx_propagation_hits, last_best_number, tracker_roles) = self
                .peer_tracker
                .lookup(&id)
                .unwrap_or((0, 0, 0, 0, String::new()));
            details_by_id.insert(
                id.clone(),
                FilterPeerDetail {
                    peer_id: id.clone(),
                    addr: addrs.get(&id).cloned(),
                    roles: format!("{:?}", info.roles),
                    best_hash: format!("{:?}", info.best_hash),
                    best_number: best,
                    score,
                    first_announce_hits,
                    tx_propagation_hits,
                    last_best_number,
                    tracker_roles: if tracker_roles.is_empty() {
                        None
                    } else {
                        Some(tracker_roles)
                    },
                },
            );
            connected_rows.push((peer_id, id, best));
        }

        let connected_before = connected_rows.len() as u32;

        if connected_rows.len() <= keep_count_usize {
            let kept: Vec<String> = connected_rows.into_iter().map(|(_, id, _)| id).collect();
            let kept_details: Vec<FilterPeerDetail> = kept
                .iter()
                .filter_map(|id| details_by_id.get(id).cloned())
                .collect();
            let result = KeepTopPeersResult {
                connected_before,
                kept_count: kept.len() as u32,
                dropped_count: 0,
                kept: kept.clone(),
                dropped: Vec::new(),
            };
            if let Err(e) = write_filter_log(FilterLogEntry {
                timestamp: filter_timestamp(),
                trigger: trigger.into(),
                interval_secs,
                keep_count,
                connected_before: result.connected_before,
                kept_count: result.kept_count,
                dropped_count: result.dropped_count,
                kept: kept_details,
                dropped: Vec::new(),
            }) {
                log::warn!(target: "bot::peers", "filter log write failed: {e}");
            }
            return Ok(result);
        }

        let rank_input: Vec<(String, u64)> = connected_rows
            .iter()
            .map(|(_, id, best)| (id.clone(), *best))
            .collect();
        let keep_set: std::collections::HashSet<String> = self
            .peer_tracker
            .rank_connected(&rank_input)
            .into_iter()
            .take(keep_count_usize)
            .collect();

        let mut kept = Vec::new();
        let mut dropped = Vec::new();

        for (peer_id, id, _) in connected_rows {
            if keep_set.contains(&id) {
                kept.push(id);
            } else {
                dropped.push(id.clone());
                log::info!(
                    target: "bot::peers",
                    "dropping peer {id} (outside top {keep_count_usize})",
                );
                self.network
                    .report_peer(peer_id, ReputationChange::new_fatal("bot_keepTopPeers"));
                self.network
                    .disconnect_peer(peer_id, self.block_announces_protocol.clone());
            }
        }

        log::info!(
            target: "bot::peers",
            "keep_top_peers: kept {} dropped {} (connected was {connected_before})",
            kept.len(),
            dropped.len(),
        );

        let kept_details: Vec<FilterPeerDetail> = kept
            .iter()
            .filter_map(|id| details_by_id.get(id).cloned())
            .collect();
        let dropped_details: Vec<FilterPeerDetail> = dropped
            .iter()
            .filter_map(|id| details_by_id.get(id).cloned())
            .collect();

        let result = KeepTopPeersResult {
            connected_before,
            kept_count: kept.len() as u32,
            dropped_count: dropped.len() as u32,
            kept: kept.clone(),
            dropped: dropped.clone(),
        };

        if let Err(e) = write_filter_log(FilterLogEntry {
            timestamp: filter_timestamp(),
            trigger: trigger.into(),
            interval_secs,
            keep_count,
            connected_before: result.connected_before,
            kept_count: result.kept_count,
            dropped_count: result.dropped_count,
            kept: kept_details,
            dropped: dropped_details,
        }) {
            log::warn!(target: "bot::peers", "filter log write failed: {e}");
        }

        Ok(result)
    }

    /// Same as [`Self::keep_top`] but tagged as an auto-filter run in the log.
    pub(crate) async fn keep_top_auto(
        &self,
        keep_count: u32,
        interval_secs: u64,
    ) -> Result<KeepTopPeersResult, String> {
        self.keep_top_with_log(keep_count, "auto", Some(interval_secs))
            .await
    }

    /// Replace all sync reserved peers with the multiaddrs listed in `path`.
    ///
    /// The file is one multiaddr per line (blank lines and `#` comments are ignored).
    /// Existing reserved peers are removed first, then each unique peer from the file
    /// is added via [`sc_network::NetworkPeers::add_reserved_peer`].
    pub async fn set_reserved_from_file(&self, path: &str) -> Result<SetReservedPeersResult, String> {
        use std::collections::HashSet;

        let peers = parse_reserved_peers_file(path)?;

        let current = self
            .network
            .reserved_peers()
            .await
            .map_err(|_| "network worker unavailable".to_string())?;

        let removed_count = current.len() as u32;
        for peer_id in current {
            self.network.remove_reserved_peer(peer_id);
        }

        let mut seen = HashSet::new();
        let mut added = Vec::new();
        let mut peer_ids = Vec::new();

        for peer in peers {
            if !seen.insert(peer.peer_id) {
                continue;
            }
            self.network
                .add_reserved_peer(peer.clone())
                .map_err(|e| format!("add_reserved_peer({peer}): {e}"))?;
            peer_ids.push(peer.peer_id.to_base58());
            added.push(String::from(peer));
        }

        log::info!(
            target: "bot::peers",
            "set_reserved_from_file({path}): removed {removed_count}, added {}",
            added.len(),
        );

        Ok(SetReservedPeersResult {
            removed_count,
            added_count: added.len() as u32,
            peers: added,
            peer_ids,
        })
    }
}

fn parse_reserved_peers_file(path: &str) -> Result<Vec<sc_network::config::MultiaddrWithPeerId>, String> {
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

fn filter_log_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../filter_log")
}

fn filter_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return "unknown".into();
    };

    let total_secs = duration.as_secs();
    let days = total_secs / 86_400;
    let rem = total_secs % 86_400;
    let hours = rem / 3_600;
    let minutes = (rem % 3_600) / 60;
    let seconds = rem % 60;

    // Unix epoch date in UTC (good enough for unique log filenames).
    let (year, month, day) = epoch_days_to_ymd(days);
    format!(
        "{year:04}-{month:02}-{day:02}_{hours:02}-{minutes:02}-{seconds:02}"
    )
}

fn epoch_days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970u64;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let year_days = if leap { 366 } else { 365 };
        if days < year_days {
            break;
        }
        days -= year_days;
        year += 1;
    }

    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1u64;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }

    (year, month, days + 1)
}

fn write_filter_log(entry: FilterLogEntry) -> Result<String, String> {
    let dir = filter_log_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("create filter_log dir {:?}: {e}", dir))?;

    let filename = format!("{}.json", entry.timestamp);
    let path = dir.join(&filename);
    let body = serde_json::to_string_pretty(&entry)
        .map_err(|e| format!("serialize filter log: {e}"))?;
    std::fs::write(&path, body).map_err(|e| format!("write {:?}: {e}", path))?;

    log::info!(
        target: "bot::peers",
        "filter log written: {:?} (kept={}, dropped={})",
        path,
        entry.kept_count,
        entry.dropped_count,
    );

    Ok(path.display().to_string())
}

/// One known network peer from `network_state` (connected or previously seen).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NetworkPeerRow {
    pub peer_id: String,
    pub connected: bool,
    pub multiaddr: Option<String>,
    pub roles: Option<String>,
    pub announce_score: u64,
    pub first_announce_hits: u64,
    pub tx_propagation_hits: u64,
}

fn multiaddr_from_known(
    peer_id: &str,
    addrs: &HashMap<String, String>,
    known: impl IntoIterator<Item = impl ToString>,
) -> Option<String> {
    addrs.get(peer_id).cloned().or_else(|| {
        known
            .into_iter()
            .next()
            .map(|addr| {
                let addr = addr.to_string();
                if addr.contains("/p2p/") {
                    addr
                } else {
                    format!("{}/p2p/{}", addr.trim_end_matches('/'), peer_id)
                }
            })
    })
}

/// Snapshot connected + previously seen peers with addresses and tracker scores.
pub async fn collect_network_peers(
    network: &dyn sc_network::NetworkStatusProvider,
    sync: &sc_network_sync::SyncingService<node_subtensor_runtime::opaque::Block>,
    tracker: &PeerTracker,
) -> Result<Vec<NetworkPeerRow>, String> {
    let addrs = network.connected_peer_addresses().await;
    let state = network
        .network_state()
        .await
        .map_err(|_| "network worker unavailable".to_string())?;

    let connected_roles: HashMap<String, String> = sync
        .peers_info()
        .await
        .map(|peers| {
            peers
                .into_iter()
                .map(|(peer_id, info)| (peer_id.to_base58(), format!("{:?}", info.roles)))
                .collect()
        })
        .unwrap_or_default();

    let mut rows = Vec::new();

    for (peer_id, peer) in state.connected_peers {
        let (announce_score, first_announce_hits, tx_hits, _, _) =
            tracker.lookup(&peer_id).unwrap_or((0, 0, 0, 0, String::new()));
        rows.push(NetworkPeerRow {
            peer_id: peer_id.clone(),
            connected: true,
            multiaddr: multiaddr_from_known(&peer_id, &addrs, peer.known_addresses.iter()),
            roles: connected_roles.get(&peer_id).cloned(),
            announce_score,
            first_announce_hits,
            tx_propagation_hits: tx_hits,
        });
    }

    for (peer_id, peer) in state.not_connected_peers {
        let (announce_score, first_announce_hits, tx_hits, _, _) =
            tracker.lookup(&peer_id).unwrap_or((0, 0, 0, 0, String::new()));
        rows.push(NetworkPeerRow {
            peer_id: peer_id.clone(),
            connected: false,
            multiaddr: multiaddr_from_known(&peer_id, &addrs, peer.known_addresses.iter()),
            roles: connected_roles.get(&peer_id).cloned(),
            announce_score,
            first_announce_hits,
            tx_propagation_hits: tx_hits,
        });
    }

    rows.sort_by(|a, b| {
        b.connected
            .cmp(&a.connected)
            .then_with(|| b.announce_score.cmp(&a.announce_score))
            .then_with(|| a.peer_id.cmp(&b.peer_id))
    });

    Ok(rows)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
