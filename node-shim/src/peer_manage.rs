//! Unified peer management: system (origin) peers + custom peers with mode-based tx propagation.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use node_subtensor_runtime::opaque::Block;
use sc_network::config::MultiaddrWithPeerId;
use sc_network::{
    event::{DhtEvent, Event},
    service::traits::NetworkDHTProvider,
    NetworkEventStream, NetworkPeers, NetworkStatusProvider, PeerId, ProtocolName,
    ReputationChange,
};
use sc_network_sync::{SyncEvent, SyncEventStream, SyncingService};
use sc_service::SpawnTaskHandle;
use subtensor_ipc::PeerManageMode;

use crate::ipc::IpcManager;
use crate::peer_scoreboard::{PeerScoreboard, PeerScoreEntry};
use crate::config_paths::disable_peers_file;
use crate::peers::{
    connected_peer_addresses, connected_peer_details, connected_peer_directions,
    parse_disable_peers_file, parse_reserved_peers_file, write_disable_peers_file,
    ConnectedSnapshot, NetworkPeerDetails, PeerTracker, PeerTrackerInfo, peer_is_connected,
};

/// Status snapshot for RPC.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerManageStatus {
    pub mode: String,
    pub checking_ms: u64,
    pub sleep_ms: u64,
    pub normal_peers_enabled: bool,
    pub peer_log_enabled: bool,
    pub peer_log_path: Option<String>,
    pub custom_peer_count: u32,
    /// Custom peers with libp2p + sync (tx stream can be open).
    pub custom_open_stream: u32,
    /// Connected peers that are not reserved (discovered / normal).
    pub normal_connected: u32,
    pub custom_connected: u32,
    pub system_reserved_count: u32,
    /// All libp2p-connected peers.
    pub connected_total: u32,
    pub custom_peers: Vec<CustomPeerRow>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClearNormalPeersResult {
    pub disconnected: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CustomPeerRow {
    pub peer_id: String,
    pub multiaddr: String,
    pub connected: bool,
    pub libp2p: bool,
    pub sync: bool,
    pub tx_reserved: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConnectFileResult {
    pub loaded: u32,
    pub peer_ids: Vec<String>,
    pub multiaddrs: Vec<String>,
}

/// One connected (or recently seen) peer for [`PeerManager::get_peer_list`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerListEntry {
    pub peer_id: String,
    pub connected: bool,
    pub sync: bool,
    pub libp2p: bool,
    /// `in` = remote dialed us, `out` = we dialed them.
    pub direction: Option<String>,
    pub multiaddr: Option<String>,
    /// Multiaddr from custom/system peer registration, if any.
    pub registered_multiaddr: Option<String>,
    pub known_addresses: Vec<String>,
    pub role: Option<String>,
    pub version: Option<String>,
    pub best_hash: Option<String>,
    pub best_number: Option<u64>,
    pub reputation: i32,
    pub latest_ping_ms: Option<u64>,
    pub tx_reserved: bool,
    pub custom: bool,
    pub reserved: bool,
    pub system_target: bool,
    pub network_reserved: bool,
    pub disabled: bool,
    pub peer_log_seen: bool,
    pub scores: PeerScoreEntry,
    pub tracker: Option<PeerTrackerInfo>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SetDisablePeersResult {
    pub applied: u32,
    pub disconnected: u32,
    pub disabled_peers: Vec<String>,
    pub invalid_peer_ids: Vec<String>,
}

/// One peer returned by DHT `find_closest_peers`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClosestPeerEntry {
    pub peer_id: String,
    pub multiaddrs: Vec<String>,
}

/// Result of DHT lookup for peers closest to a target peer id.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FindClosestPeersResult {
    pub target: String,
    pub peers: Vec<ClosestPeerEntry>,
}

#[derive(Clone, Debug, Default)]
struct StoredPeerInfo {
    multiaddr: Option<String>,
    role: Option<String>,
    sync: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PeerCounts {
    custom_total: u32,
    custom_open_stream: u32,
    custom_not_connected: u32,
    normal_connected: u32,
    connected_total: u32,
}

/// Manages custom + system peer dials and propagation mode.
pub struct PeerManager {
    sync: Arc<SyncingService<Block>>,
    network: Arc<dyn NetworkPeers + Send + Sync>,
    network_status: Arc<dyn NetworkStatusProvider + Send + Sync>,
    block_announces_protocol: ProtocolName,
    transactions_protocol: ProtocolName,
    custom_peers: Arc<RwLock<Vec<MultiaddrWithPeerId>>>,
    system_targets: Arc<RwLock<Vec<MultiaddrWithPeerId>>>,
    custom_peer_ids: Arc<RwLock<HashSet<PeerId>>>,
    mode: AtomicU8,
    checking_ms: AtomicU64,
    sleep_ms: AtomicU64,
    generation: AtomicU64,
    skip_until: Arc<RwLock<HashMap<PeerId, Instant>>>,
    network_reserved: Arc<RwLock<HashSet<PeerId>>>,
    tx_reserved: Arc<RwLock<HashSet<PeerId>>>,
    normal_peers_enabled: AtomicBool,
    peer_log_enabled: AtomicBool,
    peer_log_path: RwLock<Option<String>>,
    logged_peers: RwLock<HashSet<PeerId>>,
    /// Dialable multiaddrs indexed by peer id (custom/system peers, DHT, libp2p network_state).
    peer_addresses: RwLock<HashMap<PeerId, String>>,
    /// Known peers (multiaddr + role), updated on sync connect and address discovery.
    known_peers: RwLock<HashMap<PeerId, StoredPeerInfo>>,
    network_events: Arc<dyn NetworkEventStream + Send + Sync>,
    dht: Arc<dyn NetworkDHTProvider + Send + Sync>,
    ipc: RwLock<Option<Arc<IpcManager>>>,
    scoreboard: Arc<PeerScoreboard>,
    disabled_peers: Arc<RwLock<HashSet<PeerId>>>,
    /// Cached connection direction per peer (`in` / `out`), used when network_state is empty.
    peer_directions: RwLock<HashMap<PeerId, String>>,
    peer_tracker: RwLock<Option<Arc<PeerTracker>>>,
}

impl PeerManager {
    pub fn new(
        sync: Arc<SyncingService<Block>>,
        network: Arc<dyn NetworkPeers + Send + Sync>,
        network_status: Arc<dyn NetworkStatusProvider + Send + Sync>,
        network_events: Arc<dyn NetworkEventStream + Send + Sync>,
        dht: Arc<dyn NetworkDHTProvider + Send + Sync>,
        block_announces_protocol: ProtocolName,
        transactions_protocol: ProtocolName,
        scoreboard: Arc<PeerScoreboard>,
    ) -> Self {
        Self {
            sync,
            network,
            network_status,
            network_events,
            dht,
            block_announces_protocol,
            transactions_protocol,
            custom_peers: Arc::new(RwLock::new(Vec::new())),
            system_targets: Arc::new(RwLock::new(Vec::new())),
            custom_peer_ids: Arc::new(RwLock::new(HashSet::new())),
            mode: AtomicU8::new(PeerManageMode::Both.as_u8()),
            checking_ms: AtomicU64::new(5_000),
            sleep_ms: AtomicU64::new(30_000),
            generation: AtomicU64::new(0),
            skip_until: Arc::new(RwLock::new(HashMap::new())),
            network_reserved: Arc::new(RwLock::new(HashSet::new())),
            tx_reserved: Arc::new(RwLock::new(HashSet::new())),
            normal_peers_enabled: AtomicBool::new(true),
            peer_log_enabled: AtomicBool::new(false),
            peer_log_path: RwLock::new(None),
            logged_peers: RwLock::new(HashSet::new()),
            peer_addresses: RwLock::new(HashMap::new()),
            known_peers: RwLock::new(HashMap::new()),
            ipc: RwLock::new(None),
            scoreboard,
            disabled_peers: Arc::new(RwLock::new(HashSet::new())),
            peer_directions: RwLock::new(HashMap::new()),
            peer_tracker: RwLock::new(None),
        }
    }

    pub fn set_peer_tracker(&self, tracker: Arc<PeerTracker>) {
        *self.peer_tracker.write().expect("poisoned") = Some(tracker);
    }

    pub fn is_disabled(&self, peer_id: &PeerId) -> bool {
        self.disabled_peers.read().expect("poisoned").contains(peer_id)
    }

    pub fn disabled_peer_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .disabled_peers
            .read()
            .expect("poisoned")
            .iter()
            .map(|id| id.to_base58())
            .collect();
        ids.sort();
        ids
    }

    /// Load disabled peers from file and apply bans/disconnects.
    pub fn load_disabled_peers_from_file(&self, path: &str) -> Result<u32, String> {
        let peers = parse_disable_peers_file(path)?;
        *self.disabled_peers.write().expect("poisoned") =
            peers.iter().copied().collect();
        for peer_id in &peers {
            self.apply_disabled_peer(*peer_id);
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        if !peers.is_empty() {
            log::info!(
                target: "bot::peer_manage",
                "loaded {} disabled peer(s) from {path}",
                peers.len(),
            );
        }
        Ok(peers.len() as u32)
    }

    /// Replace the disabled peer set, persist to file, and drop matching connections.
    pub async fn set_disable_peers(
        &self,
        peer_ids: Vec<String>,
    ) -> Result<SetDisablePeersResult, String> {
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        let mut parsed = HashSet::new();

        for id_str in peer_ids {
            let trimmed = id_str.trim();
            if trimmed.is_empty() {
                continue;
            }
            match trimmed.parse::<PeerId>() {
                Ok(id) => {
                    valid.push(trimmed.to_string());
                    parsed.insert(id);
                }
                Err(_) => invalid.push(id_str),
            }
        }

        valid.sort();
        let disable_path = disable_peers_file();
        write_disable_peers_file(
            disable_path.to_str().ok_or_else(|| "invalid disable peers path".to_string())?,
            &valid,
        )?;
        *self.disabled_peers.write().expect("poisoned") = parsed.clone();
        self.generation.fetch_add(1, Ordering::SeqCst);

        let snapshot = self.connected_snapshot().await;
        let mut disconnected = 0u32;
        for peer_id in &parsed {
            if snapshot.sync_peers.contains(peer_id)
                || peer_is_connected(&snapshot, peer_id)
            {
                disconnected += 1;
            }
            self.apply_disabled_peer(*peer_id);
        }

        log::info!(
            target: "bot::peer_manage",
            "disabled peers updated: {} peer(s), disconnected {disconnected}",
            parsed.len(),
        );

        Ok(SetDisablePeersResult {
            applied: parsed.len() as u32,
            disconnected,
            disabled_peers: valid,
            invalid_peer_ids: invalid,
        })
    }

    /// Replace disabled peers from `path`, persist to `disable_peers.txt`, drop and ban matching peers.
    pub async fn set_disable_peers_from_file(
        &self,
        path: &str,
    ) -> Result<SetDisablePeersResult, String> {
        let peers = parse_disable_peers_file(path)?;
        let mut valid: Vec<String> = peers.iter().map(|id| id.to_base58()).collect();
        valid.sort();
        self.set_disable_peers(valid.clone()).await
    }

    fn apply_disabled_peer(&self, peer_id: PeerId) {
        self.network.report_peer(
            peer_id,
            ReputationChange::new_fatal("disabled by operator"),
        );
        self.network
            .disconnect_peer(peer_id, self.block_announces_protocol.clone());
        self.network
            .disconnect_peer(peer_id, self.transactions_protocol.clone());
        self.network.remove_reserved_peer(peer_id);
        let _ = self.network.remove_peers_from_reserved_set(
            self.transactions_protocol.clone(),
            vec![peer_id],
        );
        self.network_reserved.write().expect("poisoned").remove(&peer_id);
        self.tx_reserved.write().expect("poisoned").remove(&peer_id);
        self.skip_until.write().expect("poisoned").remove(&peer_id);
    }

    fn mark_peer_direction_outbound(&self, peer_id: PeerId) {
        self.peer_directions
            .write()
            .expect("poisoned")
            .insert(peer_id, "out".into());
    }

    fn clear_peer_direction(&self, peer_id: PeerId) {
        self.peer_directions.write().expect("poisoned").remove(&peer_id);
    }

    async fn resolve_peer_direction(&self, peer_id: &PeerId) -> Option<String> {
        if let Some(dir) = self.peer_directions.read().expect("poisoned").get(peer_id) {
            return Some(dir.clone());
        }
        let directions = connected_peer_directions(self.network_status.as_ref()).await;
        directions.get(&peer_id.to_base58()).cloned()
    }

    pub async fn connected_peer_ids(&self) -> Vec<String> {
        self.sync
            .peers_info()
            .await
            .map(|peers| peers.into_iter().map(|(id, _)| id.to_base58()).collect())
            .unwrap_or_default()
    }

    fn upsert_known_peer(
        &self,
        peer_id: PeerId,
        multiaddr: Option<String>,
        role: Option<String>,
        sync: bool,
    ) {
        let mut peers = self.known_peers.write().expect("poisoned");
        let entry = peers.entry(peer_id).or_default();
        if let Some(addr) = multiaddr {
            entry.multiaddr = Some(addr);
        }
        if let Some(role) = role {
            entry.role = Some(role);
        }
        if sync {
            entry.sync = true;
        }
    }

    fn mark_peer_disconnected(&self, peer_id: PeerId) {
        self.scoreboard.record_disconnect(&peer_id.to_base58());
        self.known_peers.write().expect("poisoned").remove(&peer_id);
        self.clear_peer_direction(peer_id);
    }

    fn record_peer_address(&self, peer_id: PeerId, multiaddr: String) {
        if !crate::peers::is_dialable_multiaddr(&multiaddr) {
            return;
        }
        self.upsert_known_peer(peer_id, Some(multiaddr.clone()), None, false);
        self.peer_addresses
            .write()
            .expect("poisoned")
            .insert(peer_id, multiaddr);
    }

    pub fn set_ipc(&self, ipc: Arc<IpcManager>) {
        *self.ipc.write().expect("poisoned") = Some(ipc);
    }

    pub fn peer_log_enabled(&self) -> bool {
        self.peer_log_enabled.load(Ordering::SeqCst)
    }

    pub fn peer_log_path(&self) -> Option<String> {
        self.peer_log_path.read().expect("poisoned").clone()
    }

    /// Append newly seen peers to a log file (works even when normal peers are disabled).
    pub fn enable_log_peer(&self, path: Option<String>) {
        let path = path.unwrap_or_else(|| "peer_log.txt".into());
        *self.peer_log_path.write().expect("poisoned") = Some(path.clone());
        self.peer_log_enabled.store(true, Ordering::SeqCst);
        let reserved_path = crate::config_paths::reserved_peers_file();
        let mut candidates = vec![
            reserved_path.to_string_lossy().into_owned(),
            "reserved.txt".into(),
            "bot/reserved.txt".into(),
        ];
        candidates.dedup();
        for candidate in candidates {
            match self.preload_peer_addresses_from_file(&candidate) {
                Ok(n) if n > 0 => {
                    log::info!(
                        target: "bot::peer_manage",
                        "peer log enabled: {path} (preloaded {n} address(es) from {candidate})",
                    );
                    return;
                }
                Ok(_) => {}
                Err(e) => log::debug!(
                    target: "bot::peer_manage",
                    "peer log preload {candidate}: {e}",
                ),
            }
        }
        log::info!(target: "bot::peer_manage", "peer log enabled: {path}");
    }

    /// Index dialable multiaddrs from a reserved-peer file (no connections opened).
    pub fn preload_peer_addresses_from_file(&self, path: &str) -> Result<u32, String> {
        let peers = parse_reserved_peers_file(path)?;
        Ok(self.preload_peer_addresses(peers))
    }

    /// Index dialable multiaddrs for later peer-log / find_peer resolution.
    pub fn preload_peer_addresses(&self, peers: impl IntoIterator<Item = MultiaddrWithPeerId>) -> u32 {
        let mut count = 0u32;
        for peer in peers {
            let peer_id: PeerId = peer.peer_id.into();
            let addr = String::from(peer.clone());
            if crate::peers::is_dialable_multiaddr(&addr) {
                self.record_peer_address(peer_id, addr);
                count += 1;
            }
        }
        count
    }

    fn lookup_registered_multiaddr(&self, peer_id: &PeerId) -> Option<String> {
        if let Some(addr) = self.peer_addresses.read().expect("poisoned").get(peer_id) {
            if crate::peers::is_dialable_multiaddr(addr) {
                return Some(addr.clone());
            }
        }
        for peer in self
            .custom_peers
            .read()
            .expect("poisoned")
            .iter()
            .chain(self.system_targets.read().expect("poisoned").iter())
        {
            if PeerId::from(peer.peer_id) == *peer_id {
                let addr = String::from(peer.clone());
                if crate::peers::is_dialable_multiaddr(&addr) {
                    self.record_peer_address(*peer_id, addr.clone());
                    return Some(addr);
                }
            }
        }
        None
    }

    async fn resolve_peer_multiaddr(&self, peer_id: &PeerId) -> Option<String> {
        if let Some(addr) = self.lookup_registered_multiaddr(peer_id) {
            return Some(addr);
        }
        self.dht.find_closest_peers(*peer_id);
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(15) {
            if let Some(addr) = self.lookup_registered_multiaddr(peer_id) {
                return Some(addr);
            }
            let id = peer_id.to_base58();
            if let Some(addr) = connected_peer_addresses(self.network_status.as_ref())
                .await
                .get(&id)
            {
                if crate::peers::is_dialable_multiaddr(addr) {
                    self.record_peer_address(*peer_id, addr.clone());
                    return Some(addr.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        None
    }

    async fn is_sync_peer(&self, peer_id: &PeerId) -> bool {
        self.sync
            .peers_info()
            .await
            .ok()
            .is_some_and(|peers| peers.iter().any(|(id, _)| id == peer_id))
    }

    fn p2p_only_multiaddr(peer_id: &PeerId) -> String {
        format!("/p2p/{}", peer_id.to_base58())
    }

    /// Resolve multiaddr for peer log; sync peers get a `/p2p/<id>` fallback (litep2p often has no IP).
    async fn resolve_peer_multiaddr_for_log(&self, peer_id: &PeerId) -> Option<String> {
        if let Some(addr) = self.resolve_peer_multiaddr(peer_id).await {
            return Some(addr);
        }
        if self.is_sync_peer(peer_id).await {
            let addr = Self::p2p_only_multiaddr(peer_id);
            log::info!(
                target: "bot::peer_manage",
                "peer log using p2p-only multiaddr for sync peer {}",
                peer_id.to_base58(),
            );
            return Some(addr);
        }
        None
    }

    async fn log_peer_when_resolved(self: Arc<Self>, remote: PeerId, kind: String, path: String) {
        let peer_id = remote.to_base58();
        let Some(multiaddr) = self.resolve_peer_multiaddr_for_log(&remote).await else {
            log::warn!(
                target: "bot::peer_manage",
                "peer log skipped (not in sync set, no dialable multiaddr): {peer_id}",
            );
            return;
        };
        {
            let mut seen = self.logged_peers.write().expect("poisoned");
            if !seen.insert(remote) {
                return;
            }
        }
        self.upsert_known_peer(remote, Some(multiaddr.clone()), None, true);

        let line = format!(
            "{} {kind} {peer_id} {multiaddr}\n",
            peer_log_timestamp(),
        );
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut file) => {
                if file.write_all(line.as_bytes()).is_err() {
                    log::warn!(target: "bot::peer_manage", "failed to write peer log {path}");
                    return;
                }
            }
            Err(e) => {
                log::warn!(target: "bot::peer_manage", "failed to open peer log {path}: {e}");
                return;
            }
        }
        log::info!(
            target: "bot::peer_manage",
            "logged new peer ({kind}): {peer_id} {multiaddr}",
        );
        if let Some(ipc) = self.ipc.read().expect("poisoned").clone() {
            ipc.notify_find_peer(peer_id, multiaddr);
        }
    }

    pub fn disable_log_peer(&self) {
        self.peer_log_enabled.store(false, Ordering::SeqCst);
        log::info!(target: "bot::peer_manage", "peer log disabled");
    }

    async fn connected_snapshot(&self) -> ConnectedSnapshot {
        crate::peers::connected_snapshot(
            &self.sync,
            Arc::clone(&self.network),
            Arc::clone(&self.network_status),
        )
        .await
    }

    pub fn normal_peers_enabled(&self) -> bool {
        self.normal_peers_enabled.load(Ordering::SeqCst)
    }

    /// Allow discovered (non-reserved) peers to connect for sync.
    pub fn enable_normal_peers(&self) {
        self.normal_peers_enabled.store(true, Ordering::SeqCst);
        self.network.accept_unreserved_peers();
        self.generation.fetch_add(1, Ordering::SeqCst);
        log::info!(target: "bot::peer_manage", "normal peers enabled");
    }

    /// Disconnect connected normal peers and deny new non-reserved connections.
    pub async fn disable_normal_peers(&self) -> Result<ClearNormalPeersResult, String> {
        self.normal_peers_enabled.store(false, Ordering::SeqCst);
        let result = self.clear_normal_peers().await?;
        self.network.deny_unreserved_peers();
        self.generation.fetch_add(1, Ordering::SeqCst);
        log::info!(
            target: "bot::peer_manage",
            "normal peers disabled (disconnected {})",
            result.disconnected,
        );
        Ok(result)
    }

    /// Disconnect all connected peers that are not in the reserved set.
    pub async fn clear_normal_peers(&self) -> Result<ClearNormalPeersResult, String> {
        let snapshot = self.connected_snapshot().await;
        let reserved = self.reserved_peer_ids().await?;
        let mut disconnected = 0u32;

        for peer_id in &snapshot.sync_peers {
            if reserved.contains(peer_id) {
                continue;
            }
            self.network
                .disconnect_peer(*peer_id, self.block_announces_protocol.clone());
            self.network
                .disconnect_peer(*peer_id, self.transactions_protocol.clone());
            disconnected += 1;
        }

        if disconnected > 0 {
            log::info!(
                target: "bot::peer_manage",
                "cleared {disconnected} normal peer connection(s)",
            );
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        Ok(ClearNormalPeersResult { disconnected })
    }

    async fn reserved_peer_ids(&self) -> Result<HashSet<PeerId>, String> {
        let mut reserved = self.custom_peer_ids.read().expect("poisoned").clone();
        if let Ok(list) = self.network.reserved_peers().await {
            reserved.extend(list);
        }
        Ok(reserved)
    }

    /// Replace system reserved dial targets (after `set_reserved_from_file`).
    pub fn set_system_targets(&self, peers: Vec<MultiaddrWithPeerId>) {
        let count = peers.len();
        let peer_ids: HashSet<PeerId> = peers.iter().map(|p| p.peer_id.into()).collect();
        for peer in &peers {
            self.record_peer_address(peer.peer_id.into(), String::from(peer.clone()));
        }
        *self.system_targets.write().expect("poisoned") = peers;
        let mut network_reserved = peer_ids.clone();
        network_reserved.extend(self.custom_peer_ids.read().expect("poisoned").iter().copied());
        *self.network_reserved.write().expect("poisoned") = network_reserved;
        let mut tx_reserved = peer_ids;
        tx_reserved.extend(self.custom_peer_ids.read().expect("poisoned").iter().copied());
        *self.tx_reserved.write().expect("poisoned") = tx_reserved;
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.skip_until.write().expect("poisoned").clear();
            log::info!(
                target: "bot::peers",
                "system dial targets updated ({count} peer(s))",
            );
    }

    pub fn mode(&self) -> PeerManageMode {
        PeerManageMode::from_u8(self.mode.load(Ordering::SeqCst)).unwrap_or(PeerManageMode::Both)
    }

    pub fn set_mode(&self, mode: PeerManageMode) {
        self.mode.store(mode.as_u8(), Ordering::SeqCst);
        log::info!(
            target: "bot::peer_manage",
            "propagation mode set to {}",
            mode.as_u8()
        );
    }

    pub fn set_checking_time(&self, checking_ms: u64, sleep_ms: u64) {
        let checking_ms = checking_ms.max(500);
        let sleep_ms = sleep_ms.max(500);
        self.checking_ms.store(checking_ms, Ordering::SeqCst);
        self.sleep_ms.store(sleep_ms, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        log::info!(
            target: "bot::peer_manage",
            "dial loop timing: check every {checking_ms}ms, sleep {sleep_ms}ms when connected",
        );
    }

    pub fn custom_peer_ids(&self) -> HashSet<PeerId> {
        self.custom_peer_ids.read().expect("poisoned").clone()
    }

    /// Add a custom peer and register on sync + tx reserved sets.
    pub async fn connect(&self, multiaddr: &str) -> Result<String, String> {
        self.connect_inner(multiaddr, true).await
    }

    async fn connect_inner(&self, multiaddr: &str, log_each: bool) -> Result<String, String> {
        let peer: MultiaddrWithPeerId = multiaddr
            .parse()
            .map_err(|e| format!("parse multiaddr: {e}"))?;
        let peer_id: PeerId = peer.peer_id.into();
        let id = peer_id.to_base58();

        if self.is_disabled(&peer_id) {
            return Err(format!("peer {id} is disabled"));
        }

        {
            let mut peers = self.custom_peers.write().expect("poisoned");
            if !peers.iter().any(|p| p.peer_id == peer.peer_id) {
                peers.push(peer.clone());
            }
        }
        self.custom_peer_ids.write().expect("poisoned").insert(peer_id);
        self.record_peer_address(peer_id, String::from(peer.clone()));
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.skip_until.write().expect("poisoned").remove(&peer_id);

        self.mark_peer_direction_outbound(peer_id);
        self.network
            .add_reserved_peer(peer.clone())
            .map_err(|e| format!("add_reserved_peer: {e}"))?;
        self.network_reserved
            .write()
            .expect("poisoned")
            .insert(peer_id);

        self.ensure_tx_reserved(&peer);

        if log_each {
            log::debug!(target: "bot::peer_manage", "custom peer registered: {id}");
        }
        Ok(id)
    }

    pub async fn disconnect(&self, peer_id_str: &str) -> Result<(), String> {
        let peer_id: PeerId = peer_id_str
            .parse()
            .map_err(|_| format!("invalid peer id: {peer_id_str}"))?;

        self.custom_peers
            .write()
            .expect("poisoned")
            .retain(|p| p.peer_id != peer_id.into());
        self.custom_peer_ids.write().expect("poisoned").remove(&peer_id);
        self.generation.fetch_add(1, Ordering::SeqCst);

        self.network
            .disconnect_peer(peer_id, self.block_announces_protocol.clone());
        self.network
            .disconnect_peer(peer_id, self.transactions_protocol.clone());
        self.network.remove_reserved_peer(peer_id);

        log::info!(target: "bot::peer_manage", "custom peer disconnected: {peer_id_str}");
        Ok(())
    }

    pub async fn disconnect_all(&self) -> Result<u32, String> {
        let ids: Vec<PeerId> = self
            .custom_peer_ids
            .read()
            .expect("poisoned")
            .iter()
            .copied()
            .collect();
        for id in &ids {
            let _ = self.disconnect(&id.to_base58()).await;
        }
        Ok(ids.len() as u32)
    }

    /// Register custom peers from a reserved-peer file (additive; existing connections kept).
    pub async fn connect_with_file(&self, path: &str) -> Result<ConnectFileResult, String> {
        let peers = parse_reserved_peers_file(path)?;
        let mut peer_ids = Vec::new();
        let mut multiaddrs = Vec::new();
        for peer in peers {
            let addr = String::from(peer.clone());
            let id = self.connect_inner(&addr, false).await?;
            peer_ids.push(id);
            multiaddrs.push(addr);
        }
        log::info!(
            target: "bot::peer_manage",
            "custom peers registered from {path}: {} peer(s)",
            peer_ids.len(),
        );
        Ok(ConnectFileResult {
            loaded: peer_ids.len() as u32,
            peer_ids,
            multiaddrs,
        })
    }

    pub async fn get_status(&self) -> Result<PeerManageStatus, String> {
        let custom = self.custom_peers.read().expect("poisoned").clone();
        let snapshot = self.connected_snapshot().await;
        let reserved = self.reserved_peer_ids().await?;

        let mut custom_rows = Vec::new();
        let mut custom_open_stream = 0u32;
        for peer in &custom {
            let peer_id: PeerId = peer.peer_id.into();
            let id = peer_id.to_base58();
            let sync = snapshot.sync_peers.contains(&peer_id);
            let libp2p = peer_is_connected(&snapshot, &peer_id);
            let tx_reserved = self
                .tx_reserved
                .read()
                .expect("poisoned")
                .contains(&peer_id);
            let connected = sync;
            if connected {
                custom_open_stream += 1;
            }
            custom_rows.push(CustomPeerRow {
                peer_id: id,
                multiaddr: String::from(peer.clone()),
                connected,
                libp2p,
                sync,
                tx_reserved,
            });
        }

        let normal_connected = snapshot
            .sync_peers
            .iter()
            .filter(|id| !reserved.contains(id))
            .count() as u32;

        let system_reserved = self
            .network
            .reserved_peers()
            .await
            .map(|p| p.len() as u32)
            .unwrap_or(0);

        Ok(PeerManageStatus {
            mode: format!("{:?}", self.mode()).to_lowercase(),
            checking_ms: self.checking_ms.load(Ordering::SeqCst),
            sleep_ms: self.sleep_ms.load(Ordering::SeqCst),
            normal_peers_enabled: self.normal_peers_enabled(),
            peer_log_enabled: self.peer_log_enabled(),
            peer_log_path: self.peer_log_path(),
            custom_peer_count: custom.len() as u32,
            custom_open_stream,
            normal_connected,
            custom_connected: custom_open_stream,
            system_reserved_count: system_reserved,
            connected_total: snapshot.total,
            custom_peers: custom_rows,
        })
    }

    /// Connected peers with multiaddr and role (live snapshot + stored cache).
    pub async fn get_peer_list(&self) -> Result<Vec<PeerListEntry>, String> {
        let snapshot = self.connected_snapshot().await;
        let reserved = self.reserved_peer_ids().await?;
        let custom_ids = self.custom_peer_ids.read().expect("poisoned").clone();
        let custom_peers = self.custom_peers.read().expect("poisoned").clone();
        let system_targets = self.system_targets.read().expect("poisoned").clone();
        let tx_reserved = self.tx_reserved.read().expect("poisoned").clone();
        let network_reserved = self.network_reserved.read().expect("poisoned").clone();
        let disabled = self.disabled_peers.read().expect("poisoned").clone();
        let logged_peers = self.logged_peers.read().expect("poisoned").clone();
        let address_cache = self.peer_addresses.read().expect("poisoned").clone();
        let directions = connected_peer_directions(self.network_status.as_ref()).await;
        let network_details = connected_peer_details(self.network_status.as_ref()).await;
        let peer_tracker = self.peer_tracker.read().expect("poisoned").clone();

        let sync_infos: HashMap<PeerId, sc_network_sync::types::ExtendedPeerInfo<Block>> = self
            .sync
            .peers_info()
            .await
            .map_err(|_| "sync peers_info channel closed".to_string())?
            .into_iter()
            .collect();

        let mut peer_ids: HashSet<PeerId> = snapshot.sync_peers.iter().copied().collect();
        for id in sync_infos.keys() {
            peer_ids.insert(*id);
        }
        for id_str in snapshot.addresses.keys() {
            if let Ok(id) = id_str.parse::<PeerId>() {
                peer_ids.insert(id);
            }
        }
        for id in address_cache.keys() {
            peer_ids.insert(*id);
        }

        let registered_addrs: HashMap<PeerId, String> = custom_peers
            .iter()
            .chain(system_targets.iter())
            .map(|p| (PeerId::from(p.peer_id), String::from(p.clone())))
            .collect();
        let system_target_ids: HashSet<PeerId> =
            system_targets.iter().map(|p| p.peer_id.into()).collect();

        let mut out = Vec::new();
        for peer_id in peer_ids {
            let peer_id_str = peer_id.to_base58();
            let sync = snapshot.sync_peers.contains(&peer_id);
            let libp2p = peer_is_connected(&snapshot, &peer_id);
            let connected = sync || libp2p;
            if !connected {
                continue;
            }

            let multiaddr = snapshot
                .addresses
                .get(&peer_id_str)
                .cloned()
                .or_else(|| self.lookup_registered_multiaddr(&peer_id))
                .or_else(|| address_cache.get(&peer_id).cloned())
                .or_else(|| {
                    self.known_peers
                        .read()
                        .expect("poisoned")
                        .get(&peer_id)
                        .and_then(|p| p.multiaddr.clone())
                });

            let sync_info = sync_infos.get(&peer_id);
            let role = sync_info
                .map(|info| format!("{:?}", info.roles))
                .or_else(|| {
                    self.known_peers
                        .read()
                        .expect("poisoned")
                        .get(&peer_id)
                        .and_then(|p| p.role.clone())
                });

            let best_hash = sync_info.map(|info| format!("{:?}", info.best_hash));
            let best_number = sync_info.map(|info| {
                use sp_runtime::traits::UniqueSaturatedInto;
                UniqueSaturatedInto::<u64>::unique_saturated_into(info.best_number)
            });

            if let Some(ref addr) = multiaddr {
                self.upsert_known_peer(peer_id, Some(addr.clone()), role.clone(), sync);
            }

            let direction = self
                .peer_directions
                .read()
                .expect("poisoned")
                .get(&peer_id)
                .cloned()
                .or_else(|| directions.get(&peer_id_str).cloned());

            let NetworkPeerDetails {
                version,
                latest_ping_ms,
                known_addresses,
            } = network_details
                .get(&peer_id_str)
                .cloned()
                .unwrap_or_default();

            let latest_ping_ms = latest_ping_ms.or_else(|| {
                self.scoreboard
                    .entry_for(&peer_id_str, connected)
                    .rtt_ms
            });

            let tracker = peer_tracker
                .as_ref()
                .and_then(|t| t.lookup(&peer_id_str));

            out.push(PeerListEntry {
                peer_id: peer_id_str.clone(),
                connected,
                sync,
                libp2p,
                direction,
                multiaddr,
                registered_multiaddr: registered_addrs.get(&peer_id).cloned(),
                known_addresses,
                role,
                version,
                best_hash,
                best_number,
                reputation: self.network.peer_reputation(&peer_id),
                latest_ping_ms,
                tx_reserved: tx_reserved.contains(&peer_id),
                custom: custom_ids.contains(&peer_id),
                reserved: reserved.contains(&peer_id),
                system_target: system_target_ids.contains(&peer_id),
                network_reserved: network_reserved.contains(&peer_id),
                disabled: disabled.contains(&peer_id),
                peer_log_seen: logged_peers.contains(&peer_id),
                scores: self.scoreboard.entry_for(&peer_id_str, connected),
                tracker,
            });
        }

        out.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        Ok(out)
    }

    /// DHT `find_closest_peers(target)` — returns closest peers and their multiaddrs.
    pub async fn find_closest_peers(&self, peer_id_str: &str) -> Result<FindClosestPeersResult, String> {
        let target: PeerId = peer_id_str
            .parse()
            .map_err(|_| format!("invalid peer id: {peer_id_str}"))?;

        let mut events = self
            .network_events
            .event_stream("bot-find-closest-peers");
        self.dht.find_closest_peers(target);

        let started = Instant::now();
        const TIMEOUT: Duration = Duration::from_secs(15);
        while started.elapsed() < TIMEOUT {
            let remaining = TIMEOUT.saturating_sub(started.elapsed());
            let event = match tokio::time::timeout(remaining, events.next()).await {
                Ok(Some(event)) => event,
                Ok(None) | Err(_) => break,
            };
            match event {
                Event::Dht(DhtEvent::ClosestPeersFound(found_target, peers))
                    if found_target == target =>
                {
                    let peers = peers
                        .into_iter()
                        .map(|(peer_id, addrs)| {
                            let multiaddrs: Vec<String> =
                                addrs.into_iter().map(|a| a.to_string()).collect();
                            for addr in &multiaddrs {
                                if crate::peers::is_dialable_multiaddr(addr) {
                                    self.record_peer_address(peer_id, addr.clone());
                                    break;
                                }
                            }
                            ClosestPeerEntry {
                                peer_id: peer_id.to_base58(),
                                multiaddrs,
                            }
                        })
                        .collect();
                    return Ok(FindClosestPeersResult {
                        target: target.to_base58(),
                        peers,
                    });
                }
                Event::Dht(DhtEvent::ClosestPeersNotFound(not_found)) if not_found == target => {
                    return Ok(FindClosestPeersResult {
                        target: target.to_base58(),
                        peers: vec![],
                    });
                }
                _ => {}
            }
        }

        Err(format!(
            "timeout waiting for DHT closest peers for {peer_id_str}"
        ))
    }

    async fn on_sync_peer_connected(self: Arc<Self>, peer_id: PeerId) {
        if self.is_disabled(&peer_id) {
            log::info!(
                target: "bot::peer_manage",
                "rejecting disabled peer connection: {}",
                peer_id.to_base58(),
            );
            self.apply_disabled_peer(peer_id);
            return;
        }

        let direction = self
            .resolve_peer_direction(&peer_id)
            .await
            .unwrap_or_else(|| "in".into());
        self.peer_directions
            .write()
            .expect("poisoned")
            .entry(peer_id)
            .or_insert_with(|| direction.clone());
        log::info!(
            target: "bot::peer_manage",
            "peer connected dir={direction} peer={}",
            peer_id.to_base58(),
        );

        let role = self
            .sync
            .peers_info()
            .await
            .ok()
            .and_then(|peers| {
                peers
                    .into_iter()
                    .find(|(id, _)| *id == peer_id)
                    .map(|(_, info)| format!("{:?}", info.roles))
            });
        let multiaddr = self.lookup_registered_multiaddr(&peer_id);
        self.scoreboard.record_connect(&peer_id.to_base58());
        self.upsert_known_peer(peer_id, multiaddr, role, true);
    }

    async fn backfill_known_peers(self: Arc<Self>) {
        let Ok(peers) = self.sync.peers_info().await else {
            return;
        };
        for (peer_id, info) in peers {
            let role = format!("{:?}", info.roles);
            let multiaddr = self.lookup_registered_multiaddr(&peer_id);
            self.upsert_known_peer(peer_id, multiaddr, Some(role), true);
        }
        let snapshot = self.connected_snapshot().await;
        for (peer_id_str, addr) in snapshot.addresses {
            if let Ok(peer_id) = peer_id_str.parse::<PeerId>() {
                if crate::peers::is_dialable_multiaddr(&addr) {
                    self.upsert_known_peer(
                        peer_id,
                        Some(addr),
                        None,
                        snapshot.sync_peers.contains(&peer_id),
                    );
                }
            }
        }
    }

    pub fn start(self: Arc<Self>, spawn_handle: SpawnTaskHandle) {
        if self.normal_peers_enabled() {
            self.network.accept_unreserved_peers();
        } else {
            self.network.deny_unreserved_peers();
        }
        let dial = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-manage-dial", None, async move {
            dial.run_dial_loop().await;
        });
        let log = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-log", None, async move {
            log.run_peer_log().await;
        });
        let registry = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-registry-init", None, async move {
            registry.clone().backfill_known_peers().await;
            registry.scan_unlogged_sync_peers().await;
        });
        let backfill = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-log-backfill", None, async move {
            backfill.run_peer_log_backfill().await;
        });
        let cache = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-address-cache", None, async move {
            cache.run_peer_address_cache().await;
        });
    }

    async fn run_peer_address_cache(self: Arc<Self>) {
        let mut events = self.network_events.event_stream("bot-peer-address-cache");
        loop {
            let Some(event) = events.next().await else {
                break;
            };
            let Event::Dht(DhtEvent::ClosestPeersFound(_, peers)) = event else {
                continue;
            };
            for (peer_id, addrs) in peers {
                if let Some(addr) = addrs.first() {
                    let addr = addr.to_string();
                    if crate::peers::is_dialable_multiaddr(&addr) {
                        self.upsert_known_peer(peer_id, Some(addr.clone()), None, false);
                        self.peer_addresses
                            .write()
                            .expect("poisoned")
                            .insert(peer_id, addr);
                    }
                }
            }
        }
    }

    /// Log any sync peer not yet in `logged_peers` (catches peers missed on first connect).
    async fn scan_unlogged_sync_peers(self: Arc<Self>) {
        if !self.peer_log_enabled.load(Ordering::SeqCst) {
            return;
        }
        let path = match self.peer_log_path.read().expect("poisoned").clone() {
            Some(path) => path,
            None => return,
        };
        let Ok(peers) = self.sync.peers_info().await else {
            return;
        };
        for (peer_id, _) in peers {
            if self.logged_peers.read().expect("poisoned").contains(&peer_id) {
                continue;
            }
            let custom = self.custom_peer_ids.read().expect("poisoned").contains(&peer_id);
            let kind = if custom { "custom" } else { "normal" }.to_string();
            let slf = Arc::clone(&self);
            let path = path.clone();
            tokio::spawn(async move {
                slf.log_peer_when_resolved(peer_id, kind, path).await;
            });
        }
    }

    async fn run_peer_log_backfill(self: Arc<Self>) {
        const INTERVAL: Duration = Duration::from_secs(30);
        loop {
            tokio::time::sleep(INTERVAL).await;
            self.clone().scan_unlogged_sync_peers().await;
        }
    }

    async fn run_peer_log(self: Arc<Self>) {
        // Sync peer-connected events work with both libp2p and litep2p backends.
        // `NetworkEventStream::NotificationStreamOpened` is not emitted on litep2p.
        let mut events = self.sync.event_stream("bot-peer-log");
        loop {
            let Some(event) = events.next().await else {
                break;
            };
            match event {
                SyncEvent::PeerConnected(remote) => {
                    if self.is_disabled(&remote) {
                        log::info!(
                            target: "bot::peer_manage",
                            "rejecting disabled peer connection: {}",
                            remote.to_base58(),
                        );
                        self.apply_disabled_peer(remote);
                        continue;
                    }

                    let slf = Arc::clone(&self);
                    tokio::spawn(async move {
                        slf.on_sync_peer_connected(remote).await;
                    });

                    if !self.peer_log_enabled.load(Ordering::SeqCst) {
                        continue;
                    }
                    {
                        let seen = self.logged_peers.read().expect("poisoned");
                        if seen.contains(&remote) {
                            continue;
                        }
                    }
                    let path = match self.peer_log_path.read().expect("poisoned").clone() {
                        Some(path) => path,
                        None => continue,
                    };
                    let custom = self.custom_peer_ids.read().expect("poisoned").contains(&remote);
                    let kind = if custom { "custom" } else { "normal" }.to_string();
                    let slf = Arc::clone(&self);
                    tokio::spawn(async move {
                        slf.log_peer_when_resolved(remote, kind, path).await;
                    });
                }
                SyncEvent::PeerDisconnected(remote) => {
                    self.mark_peer_disconnected(remote);
                }
            }
        }
    }

    async fn run_dial_loop(self: Arc<Self>) {
        let mut watched_generation = 0u64;
        loop {
            let generation = self.generation.load(Ordering::SeqCst);
            if generation != watched_generation {
                watched_generation = generation;
            }

            let targets = self.dial_targets();
            if targets.is_empty() {
                self.sleep_or_generation_change(generation, Duration::from_secs(5))
                    .await;
                continue;
            }

            let all_ok = self.check_and_dial(&targets, generation).await;
            let checking = Duration::from_millis(self.checking_ms.load(Ordering::SeqCst));
            let sleep = Duration::from_millis(self.sleep_ms.load(Ordering::SeqCst));
            let delay = if all_ok { sleep } else { checking };
            self.sleep_or_generation_change(generation, delay).await;
        }
    }

    async fn check_and_dial(
        &self,
        targets: &[MultiaddrWithPeerId],
        _generation: u64,
    ) -> bool {
        let snapshot = self.connected_snapshot().await;
        let reserved = self.reserved_peer_ids().await.unwrap_or_default();
        let before = self.count_peers(&snapshot, &reserved);

        if before.custom_total > 0 || before.connected_total > 0 {
            log::info!(
                target: "bot::peer_manage",
                "peers: connected_total={} custom_open_stream={}/{} normal={} custom_not_connected={}",
                before.connected_total,
                before.custom_open_stream,
                before.custom_total,
                before.normal_connected,
                before.custom_not_connected,
            );
        }

        if !self.normal_peers_enabled() {
            self.network.deny_unreserved_peers();
            if before.normal_connected > 0 {
                let _ = self.clear_normal_peers().await;
            }
        }

        let mut all_ok = before.custom_not_connected == 0;
        for peer in targets {
            let peer_id: PeerId = peer.peer_id.into();
            if self.is_disabled(&peer_id) {
                self.apply_disabled_peer(peer_id);
                continue;
            }
            self.record_peer_address(peer_id, String::from(peer.clone()));
            let connected = peer_is_connected(&snapshot, &peer_id);
            let sync = snapshot.sync_peers.contains(&peer_id);
            let tx_registered = self
                .tx_reserved
                .read()
                .expect("poisoned")
                .contains(&peer_id);

            if connected && sync && tx_registered {
                if self.should_skip(peer_id) {
                    continue;
                }
                self.mark_success_skip(peer_id);
                continue;
            }

            all_ok = false;
            if !connected || !sync {
                self.ensure_network_reserved(peer, connected);
            }
            if connected && sync && !tx_registered {
                self.ensure_tx_reserved(peer);
            }
        }

        if before.custom_total > 0 && before.custom_not_connected > 0 {
            let snapshot = self.connected_snapshot().await;
            let after = self.count_peers(&snapshot, &reserved);
            let newly = after.custom_open_stream.saturating_sub(before.custom_open_stream);
            log::info!(
                target: "bot::peer_manage",
                "peers after redial: newly_connected={} connected_total={} custom_open_stream={}/{} normal={}",
                newly,
                after.connected_total,
                after.custom_open_stream,
                after.custom_total,
                after.normal_connected,
            );
        }

        all_ok
    }

    fn count_peers(&self, snapshot: &ConnectedSnapshot, reserved: &HashSet<PeerId>) -> PeerCounts {
        let custom = self.custom_peers.read().expect("poisoned");
        let mut custom_open_stream = 0u32;
        for peer in custom.iter() {
            let peer_id: PeerId = peer.peer_id.into();
            if snapshot.sync_peers.contains(&peer_id) {
                custom_open_stream += 1;
            }
        }

        let normal_connected = snapshot
            .sync_peers
            .iter()
            .filter(|id| !reserved.contains(id))
            .count() as u32;

        let custom_total = custom.len() as u32;
        PeerCounts {
            custom_total,
            custom_open_stream,
            custom_not_connected: custom_total.saturating_sub(custom_open_stream),
            normal_connected,
            connected_total: snapshot.total,
        }
    }

    fn dial_targets(&self) -> Vec<MultiaddrWithPeerId> {
        let custom = self.custom_peers.read().expect("poisoned").clone();
        let system = self.system_targets.read().expect("poisoned").clone();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for peer in custom.into_iter().chain(system) {
            if seen.insert(peer.peer_id) {
                out.push(peer);
            }
        }
        out
    }

    fn ensure_network_reserved(&self, peer: &MultiaddrWithPeerId, connected: bool) -> bool {
        let peer_id: PeerId = peer.peer_id.into();
        if self.is_disabled(&peer_id) {
            self.apply_disabled_peer(peer_id);
            return false;
        }
        if self
            .network_reserved
            .read()
            .expect("poisoned")
            .contains(&peer_id)
            && connected
        {
            return true;
        }
        self.mark_peer_direction_outbound(peer_id);
        match self.network.add_reserved_peer(peer.clone()) {
            Ok(()) => {
                self.record_peer_address(peer_id, String::from(peer.clone()));
                self.network_reserved
                    .write()
                    .expect("poisoned")
                    .insert(peer_id);
                true
            }
            Err(err) => {
                log::warn!(
                    target: "bot::peer_manage",
                    "add_reserved_peer({peer}) failed: {err}",
                );
                false
            }
        }
    }

    /// Register peer on the transactions protocol reserved set (at most once per peer).
    fn ensure_tx_reserved(&self, peer: &MultiaddrWithPeerId) -> bool {
        let peer_id: PeerId = peer.peer_id.into();
        {
            let mut tx = self.tx_reserved.write().expect("poisoned");
            if !tx.insert(peer_id) {
                return true;
            }
        }
        let mut addrs = HashSet::new();
        addrs.insert(peer.clone().concat());
        let _ = self
            .network
            .add_peers_to_reserved_set(self.transactions_protocol.clone(), addrs);
        true
    }

    fn should_skip(&self, peer_id: PeerId) -> bool {
        let now = Instant::now();
        self.skip_until
            .read()
            .expect("poisoned")
            .get(&peer_id)
            .is_some_and(|until| now < *until)
    }

    fn mark_success_skip(&self, peer_id: PeerId) {
        let until = Instant::now() + Duration::from_millis(self.sleep_ms.load(Ordering::SeqCst));
        self.skip_until
            .write()
            .expect("poisoned")
            .insert(peer_id, until);
    }

    async fn sleep_or_generation_change(&self, generation: u64, duration: Duration) {
        let steps = duration.as_millis() / 100;
        for _ in 0..steps.max(1) {
            if self.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn peer_log_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}
