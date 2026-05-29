//! Periodically dials reserved peers until libp2p + sync + tx streams are up.

use node_subtensor_runtime::opaque::Block;
use sc_network::config::MultiaddrWithPeerId;
use sc_network::{NetworkPeers, NetworkStatusProvider, PeerId, ProtocolName};
use sc_network_sync::SyncingService;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const DIAL_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const STREAM_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const SKIP_AFTER_SUCCESS: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StreamStatus {
    libp2p: bool,
    sync: bool,
    tx_reserved: bool,
}

impl StreamStatus {
    fn all_ok(self) -> bool {
        self.libp2p && self.sync && self.tx_reserved
    }
}

/// Background dial loop for runtime reserved peers (after `set_reserved_from_file`).
pub struct ReservedDialer {
    network: Arc<dyn NetworkPeers + Send + Sync>,
    network_status: Arc<dyn NetworkStatusProvider + Send + Sync>,
    sync: Arc<SyncingService<Block>>,
    transactions_protocol: ProtocolName,
    targets: Arc<RwLock<Vec<MultiaddrWithPeerId>>>,
    generation: Arc<AtomicU64>,
    all_connected_logged: Arc<AtomicU64>,
    /// Peers fully up; skip re-check until this instant.
    skip_until: Arc<RwLock<HashMap<PeerId, Instant>>>,
    /// Peers already passed to `add_reserved_peer` (avoids "already a reserved peer" spam).
    network_reserved: Arc<RwLock<HashSet<PeerId>>>,
    /// Peers already on the tx protocol reserved set.
    tx_reserved: Arc<RwLock<HashSet<PeerId>>>,
}

impl ReservedDialer {
    pub fn new(
        network: Arc<dyn NetworkPeers + Send + Sync>,
        network_status: Arc<dyn NetworkStatusProvider + Send + Sync>,
        sync: Arc<SyncingService<Block>>,
        transactions_protocol: ProtocolName,
    ) -> Self {
        Self {
            network,
            network_status,
            sync,
            transactions_protocol,
            targets: Arc::new(RwLock::new(Vec::new())),
            generation: Arc::new(AtomicU64::new(0)),
            all_connected_logged: Arc::new(AtomicU64::new(0)),
            skip_until: Arc::new(RwLock::new(HashMap::new())),
            network_reserved: Arc::new(RwLock::new(HashSet::new())),
            tx_reserved: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Replace dial targets and restart the retry loop (call after reserved list updates).
    ///
    /// Call this **after** `set_reserved_from_file` has registered peers on network + tx
    /// protocols so the dial loop does not re-invoke `add_reserved_peer` every 5s.
    pub fn set_targets(&self, peers: Vec<MultiaddrWithPeerId>) {
        let count = peers.len();
        let peer_ids: HashSet<PeerId> = peers.iter().map(|p| p.peer_id.into()).collect();
        *self.targets.write().expect("reserved dial targets lock poisoned") = peers;
        *self
            .network_reserved
            .write()
            .expect("reserved dial network_reserved lock poisoned") = peer_ids.clone();
        *self
            .tx_reserved
            .write()
            .expect("reserved dial tx_reserved lock poisoned") = peer_ids;
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.all_connected_logged.store(0, Ordering::SeqCst);
        self.skip_until
            .write()
            .expect("reserved dial skip lock poisoned")
            .clear();
        log::info!(
            target: "bot::peers",
            "reserved dial: updated targets ({count} peer(s)), stream check every {}s (skip {}s after success)",
            STREAM_CHECK_INTERVAL.as_secs(),
            SKIP_AFTER_SUCCESS.as_secs(),
        );
    }

    pub fn start(self: Arc<Self>, spawn_handle: sc_service::SpawnTaskHandle) {
        spawn_handle.spawn("bot-reserved-dial", None, async move {
            self.run().await;
        });
    }

    async fn run(self: Arc<Self>) {
        let mut watched_generation = 0u64;
        loop {
            let generation = self.generation.load(Ordering::SeqCst);
            if generation != watched_generation {
                watched_generation = generation;
            }

            let targets = self
                .targets
                .read()
                .expect("reserved dial targets lock poisoned")
                .clone();

            if targets.is_empty() {
                self.sleep_or_generation_change(generation, STREAM_CHECK_INTERVAL)
                    .await;
                continue;
            }

            let all_ok = self.check_and_dial_targets(&targets, generation).await;
            let sleep = if all_ok {
                STREAM_CHECK_INTERVAL
            } else {
                DIAL_RETRY_INTERVAL
            };
            self.sleep_or_generation_change(generation, sleep).await;
        }
    }

    async fn peer_stream_status(
        &self,
        peer: &MultiaddrWithPeerId,
        connected_addrs: &HashMap<String, String>,
        sync_connected: &HashSet<PeerId>,
    ) -> StreamStatus {
        let peer_id: PeerId = peer.peer_id.into();
        let id = peer_id.to_base58();
        let libp2p = connected_addrs.contains_key(&id);
        let sync = sync_connected.contains(&peer_id);
        let tx_reserved = if libp2p && sync {
            self.ensure_tx_reserved(peer)
        } else {
            false
        };

        StreamStatus {
            libp2p,
            sync,
            tx_reserved,
        }
    }

    fn mark_success_skip(&self, peer_id: PeerId) {
        let until = Instant::now() + SKIP_AFTER_SUCCESS;
        self.skip_until
            .write()
            .expect("reserved dial skip lock poisoned")
            .insert(peer_id, until);
    }

    fn ensure_tx_reserved(&self, peer: &MultiaddrWithPeerId) -> bool {
        let peer_id: PeerId = peer.peer_id.into();
        if self
            .tx_reserved
            .read()
            .expect("reserved dial tx_reserved lock poisoned")
            .contains(&peer_id)
        {
            return true;
        }

        let mut addrs = HashSet::new();
        addrs.insert(peer.clone().concat());
        if self
            .network
            .add_peers_to_reserved_set(self.transactions_protocol.clone(), addrs)
            .is_ok()
        {
            self.tx_reserved
                .write()
                .expect("reserved dial tx_reserved lock poisoned")
                .insert(peer_id);
            return true;
        }
        false
    }

    /// Register on the default sync + block-announces reserved set once per peer.
    fn ensure_network_reserved(&self, peer: &MultiaddrWithPeerId) -> bool {
        let peer_id: PeerId = peer.peer_id.into();
        if self
            .network_reserved
            .read()
            .expect("reserved dial network_reserved lock poisoned")
            .contains(&peer_id)
        {
            return true;
        }

        match self.network.add_reserved_peer(peer.clone()) {
            Ok(()) => {
                self.network_reserved
                    .write()
                    .expect("reserved dial network_reserved lock poisoned")
                    .insert(peer_id);
                true
            }
            Err(err) => {
                log::warn!(
                    target: "bot::peers",
                    "reserved dial: add_reserved_peer({peer}) failed: {err}",
                );
                false
            }
        }
    }

    fn should_skip(&self, peer_id: PeerId) -> bool {
        let now = Instant::now();
        self.skip_until
            .read()
            .expect("reserved dial skip lock poisoned")
            .get(&peer_id)
            .is_some_and(|until| now < *until)
    }

    fn log_check_summary(
        &self,
        total: usize,
        on_cooldown: usize,
        newly_ok: usize,
        problems: &[(String, StreamStatus)],
        generation: u64,
    ) -> bool {
        let checked = total.saturating_sub(on_cooldown);
        let connected_now = newly_ok + on_cooldown;

        if problems.is_empty() {
            if connected_now == total {
                if self.all_connected_logged.load(Ordering::SeqCst) != generation {
                    self.all_connected_logged
                        .store(generation, Ordering::SeqCst);
                    log::info!(
                        target: "bot::peers",
                        "reserved dial: all {total} peer(s) connected (libp2p+sync+tx); re-check every {}s",
                        STREAM_CHECK_INTERVAL.as_secs(),
                    );
                }
                return true;
            }
            return true;
        }

        if checked > 0 && problems.len() == checked {
            log::info!(
                target: "bot::peers",
                "reserved dial: checked {checked} peer(s), none fully connected yet",
            );
        } else {
            log::info!(
                target: "bot::peers",
                "reserved dial: checked {checked} peer(s), {} not fully connected ({newly_ok} ok, {on_cooldown} on cooldown)",
                problems.len(),
            );
        }

        false
    }

    /// Returns `true` when every target is fully connected (and not due for re-check).
    async fn check_and_dial_targets(
        &self,
        targets: &[MultiaddrWithPeerId],
        generation: u64,
    ) -> bool {
        let total = targets.len();
        if total == 0 {
            return true;
        }

        let connected_addrs = self.network_status.connected_peer_addresses().await;
        let sync_connected: HashSet<PeerId> = match self.sync.peers_info().await {
            Ok(peers) => peers.into_iter().map(|(id, _)| id).collect(),
            Err(_) => HashSet::new(),
        };

        let on_cooldown = targets
            .iter()
            .filter(|p| self.should_skip(p.peer_id.into()))
            .count();

        let to_check = total.saturating_sub(on_cooldown);
        if to_check == 0 {
            return self.log_check_summary(total, on_cooldown, 0, &[], generation);
        }

        log::info!(
            target: "bot::peers",
            "reserved dial: check {total} peer(s) ({to_check} active, {on_cooldown} on cooldown)",
        );

        let mut newly_ok = 0usize;
        let mut problems: Vec<(String, StreamStatus)> = Vec::new();

        for peer in targets {
            let peer_id: PeerId = peer.peer_id.into();
            if self.should_skip(peer_id) {
                continue;
            }

            let id = peer_id.to_base58();
            let status = self
                .peer_stream_status(peer, &connected_addrs, &sync_connected)
                .await;

            if status.all_ok() {
                self.mark_success_skip(peer_id);
                newly_ok += 1;
                continue;
            }

            problems.push((id, status));

            if !status.libp2p {
                self.ensure_network_reserved(peer);
            } else if !self
                .network_reserved
                .read()
                .expect("reserved dial network_reserved lock poisoned")
                .contains(&peer_id)
            {
                self.ensure_network_reserved(peer);
            }

            if status.libp2p && status.sync && !status.tx_reserved {
                self.ensure_tx_reserved(peer);
            }
        }

        self.log_check_summary(total, on_cooldown, newly_ok, &problems, generation)
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
