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
use sc_network::{Event, NetworkEventStream, NetworkPeers, NetworkStatusProvider, PeerId, ProtocolName};
use sc_network_sync::SyncingService;
use sc_service::SpawnTaskHandle;
use subtensor_ipc::PeerManageMode;

use crate::ipc::IpcManager;
use crate::peers::{parse_reserved_peers_file, connected_peer_addresses, ConnectedSnapshot, peer_is_connected};

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
    network_events: Arc<dyn NetworkEventStream + Send + Sync>,
    ipc: RwLock<Option<Arc<IpcManager>>>,
}

impl PeerManager {
    pub fn new(
        sync: Arc<SyncingService<Block>>,
        network: Arc<dyn NetworkPeers + Send + Sync>,
        network_status: Arc<dyn NetworkStatusProvider + Send + Sync>,
        network_events: Arc<dyn NetworkEventStream + Send + Sync>,
        block_announces_protocol: ProtocolName,
        transactions_protocol: ProtocolName,
    ) -> Self {
        Self {
            sync,
            network,
            network_status,
            network_events,
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
            ipc: RwLock::new(None),
        }
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
        log::info!(target: "bot::peer_manage", "peer log enabled: {path}");
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

        {
            let mut peers = self.custom_peers.write().expect("poisoned");
            if !peers.iter().any(|p| p.peer_id == peer.peer_id) {
                peers.push(peer.clone());
            }
        }
        self.custom_peer_ids.write().expect("poisoned").insert(peer_id);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.skip_until.write().expect("poisoned").remove(&peer_id);

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

    pub async fn connect_with_file(&self, path: &str) -> Result<ConnectFileResult, String> {
        self.clear_normal_peers().await?;
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

    pub fn start(self: Arc<Self>, spawn_handle: SpawnTaskHandle) {
        let dial = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-manage-dial", None, async move {
            dial.run_dial_loop().await;
        });
        let log = Arc::clone(&self);
        spawn_handle.spawn("bot-peer-log", None, async move {
            log.run_peer_log().await;
        });
    }

    async fn run_peer_log(self: Arc<Self>) {
        let mut events = self.network_events.event_stream("bot-peer-log");
        loop {
            let Some(event) = events.next().await else {
                break;
            };
            if !self.peer_log_enabled.load(Ordering::SeqCst) {
                continue;
            }
            let Event::NotificationStreamOpened { remote, protocol, .. } = event else {
                continue;
            };
            if protocol != self.block_announces_protocol
                && protocol != self.transactions_protocol
            {
                continue;
            }
            {
                let mut seen = self.logged_peers.write().expect("poisoned");
                if !seen.insert(remote) {
                    continue;
                }
            }
            let path = match self.peer_log_path.read().expect("poisoned").clone() {
                Some(path) => path,
                None => continue,
            };
            let custom = self.custom_peer_ids.read().expect("poisoned").contains(&remote);
            let kind = if custom { "custom" } else { "normal" };
            let line = format!(
                "{} {kind} {}\n",
                peer_log_timestamp(),
                remote.to_base58(),
            );
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(mut file) => {
                    if file.write_all(line.as_bytes()).is_err() {
                        log::warn!(target: "bot::peer_manage", "failed to write peer log {path}");
                    }
                }
                Err(e) => {
                    log::warn!(target: "bot::peer_manage", "failed to open peer log {path}: {e}");
                }
            }
            log::info!(
                target: "bot::peer_manage",
                "logged new peer ({kind}): {}",
                remote.to_base58(),
            );

            if let Some(ipc) = self.ipc.read().expect("poisoned").clone() {
                let network_status = Arc::clone(&self.network_status);
                let peer_id = remote.to_base58();
                tokio::spawn(async move {
                    let multiaddr = connected_peer_addresses(network_status.as_ref())
                        .await
                        .get(&peer_id)
                        .cloned()
                        .unwrap_or_else(|| format!("/p2p/{peer_id}"));
                    ipc.notify_find_peer(peer_id, multiaddr);
                });
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
                self.ensure_network_reserved(peer);
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

    fn ensure_network_reserved(&self, peer: &MultiaddrWithPeerId) -> bool {
        let peer_id: PeerId = peer.peer_id.into();
        if self
            .network_reserved
            .read()
            .expect("poisoned")
            .contains(&peer_id)
        {
            return true;
        }
        match self.network.add_reserved_peer(peer.clone()) {
            Ok(()) => {
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
