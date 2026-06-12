//! Operator-configured peers/endpoints that receive forwarded first block announces.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use codec::{Decode, Encode};
use node_subtensor_runtime::opaque::Block;
use sc_network::PeerId;
use sc_network_sync::SyncingService;
use sp_blockchain::HeaderBackend;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};

use crate::announce::{current_delay_time_ms, slot_from_digest};
use crate::announce_index::AnnounceIndexTracker;
use crate::announcing_rpc::AnnouncingRpcPool;
use crate::config_paths::announcing_peers_file;
use crate::ipc::IpcManager;
use crate::metrics_log::{log_peer_announce_timing, MetricsLogControl};
use crate::peer_scoreboard::PeerScoreboard;
use crate::peers::{
    is_rpc_announcing_endpoint, parse_announcing_peers_file, write_announcing_peers_file,
};
use crate::peers::PeerTracker;
use crate::propagation_tracker::PropagationTracker;
use crate::slot_state::SlotStateStore;
use crate::transact::parse_propagation_peer_id;
use subtensor_ipc::IpcMessage;

/// How first announces are forwarded to configured targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum AnnouncingMode {
    /// Libp2p block-announces protocol (requires sync peer connection).
    Sync = 0,
    /// JSON-RPC over persistent WS / HTTP (forwards raw header immediately).
    Rpc = 1,
}

impl AnnouncingMode {
    pub fn from_u8(mode: u8) -> Option<Self> {
        match mode {
            0 => Some(Self::Sync),
            1 => Some(Self::Rpc),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Rpc => "rpc",
        }
    }
}

/// JSON-RPC payload for [`AnnouncingMode::Rpc`] forwarding.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ForwardedAnnouncePayload {
    pub header_number: u32,
    pub hash: String,
    pub parent_hash: String,
    pub header_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announcing_peer: Option<String>,
    pub delay_time_ms: u64,
}

pub trait AnnounceReceiveRpc {
    fn receive_forwarded(&self, payload: ForwardedAnnouncePayload) -> Result<bool, String>;
}

/// Result of mutating the announcing peer set.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AnnouncingPeersResult {
    pub peers: Vec<String>,
    pub invalid_peer_ids: Vec<String>,
}

/// Runtime list of forward targets and active mode.
pub struct AnnouncingPeersControl {
    mode: AtomicU32,
    targets: RwLock<Vec<String>>,
    rpc_pool: Arc<AnnouncingRpcPool>,
}

impl AnnouncingPeersControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            mode: AtomicU32::new(AnnouncingMode::Rpc.as_u8() as u32),
            targets: RwLock::new(Vec::new()),
            rpc_pool: AnnouncingRpcPool::new(),
        })
    }

    pub fn mode(&self) -> AnnouncingMode {
        AnnouncingMode::from_u8(self.mode.load(Ordering::SeqCst) as u8).unwrap_or(AnnouncingMode::Rpc)
    }

    pub fn set_mode(&self, mode: AnnouncingMode) -> Result<(), String> {
        self.mode.store(mode.as_u8() as u32, Ordering::SeqCst);
        if mode == AnnouncingMode::Rpc {
            self.rpc_pool.clear();
            for target in self.targets() {
                self.rpc_pool.connect(&target)?;
            }
        } else {
            self.rpc_pool.clear();
        }
        log::info!(
            target: "bot::announce",
            "announcing mode set to {} ({})",
            mode.as_u8(),
            mode.label(),
        );
        Ok(())
    }

    pub fn targets(&self) -> Vec<String> {
        self.targets.read().expect("poisoned").clone()
    }

    pub fn sync_peer_ids(&self) -> Vec<PeerId> {
        self.targets()
            .iter()
            .filter_map(|raw| parse_propagation_peer_id(raw).ok())
            .collect()
    }

    pub fn add(&self, target: &str) -> Result<AnnouncingPeersResult, String> {
        let target = target.trim().to_string();
        self.validate_target(&target)?;

        let mut targets = self.targets.write().expect("poisoned");
        if !targets.iter().any(|t| t == &target) {
            targets.push(target.clone());
        }
        let listed = targets.clone();
        drop(targets);
        self.after_targets_changed(&target, true)?;
        self.persist()?;
        log::info!(
            target: "bot::announce",
            "announcing target added: {target} (mode={}, total {})",
            self.mode().label(),
            self.targets().len(),
        );
        Ok(AnnouncingPeersResult {
            peers: listed,
            invalid_peer_ids: Vec::new(),
        })
    }

    pub fn remove(&self, target: &str) -> Result<AnnouncingPeersResult, String> {
        let target = target.trim().to_string();
        let mut targets = self.targets.write().expect("poisoned");
        targets.retain(|t| t != &target);
        let listed = targets.clone();
        drop(targets);
        self.after_targets_changed(&target, false)?;
        self.persist()?;
        log::info!(
            target: "bot::announce",
            "announcing target removed: {target} (total {})",
            self.targets().len(),
        );
        Ok(AnnouncingPeersResult {
            peers: listed,
            invalid_peer_ids: Vec::new(),
        })
    }

    pub fn add_from_file(&self, path: &str) -> Result<AnnouncingPeersResult, String> {
        let loaded = parse_announcing_peers_file(path)?;
        let mut added = 0u32;
        for target in loaded {
            if self.targets().iter().any(|t| t == &target) {
                continue;
            }
            self.validate_target(&target)?;
            self.targets.write().expect("poisoned").push(target.clone());
            self.after_targets_changed(&target, true)?;
            added += 1;
        }
        self.persist()?;
        log::info!(
            target: "bot::announce",
            "announcing targets loaded from {path}: added {added} (total {})",
            self.targets().len(),
        );
        Ok(AnnouncingPeersResult {
            peers: self.targets(),
            invalid_peer_ids: Vec::new(),
        })
    }

    pub fn clear_all(&self) -> Result<AnnouncingPeersResult, String> {
        self.targets.write().expect("poisoned").clear();
        self.rpc_pool.clear();
        self.persist()?;
        log::info!(target: "bot::announce", "announcing targets cleared");
        Ok(AnnouncingPeersResult {
            peers: Vec::new(),
            invalid_peer_ids: Vec::new(),
        })
    }

    pub fn load_from_default_file(&self) -> Result<u32, String> {
        let path = announcing_peers_file();
        let path = path
            .to_str()
            .ok_or_else(|| "announcing peers path is not valid UTF-8".to_string())?;
        if !std::path::Path::new(path).exists() {
            return Ok(0);
        }
        let before = self.targets().len();
        self.add_from_file(path)?;
        Ok(self.targets().len().saturating_sub(before) as u32)
    }

    fn validate_target(&self, target: &str) -> Result<(), String> {
        match self.mode() {
            AnnouncingMode::Sync => {
                parse_propagation_peer_id(target)?;
            }
            AnnouncingMode::Rpc => {
                if !is_rpc_announcing_endpoint(target) {
                    return Err(format!(
                        "rpc mode requires ws://, wss://, http://, or https:// endpoint, got {target}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn after_targets_changed(&self, target: &str, added: bool) -> Result<(), String> {
        if self.mode() != AnnouncingMode::Rpc {
            return Ok(());
        }
        if added {
            self.rpc_pool.connect(target)?;
        } else {
            self.rpc_pool.disconnect(target);
        }
        Ok(())
    }

    fn persist(&self) -> Result<(), String> {
        let path = announcing_peers_file();
        let path = path
            .to_str()
            .ok_or_else(|| "announcing peers path is not valid UTF-8".to_string())?;
        write_announcing_peers_file(path, &self.targets())
    }

    pub fn rpc_pool(&self) -> Arc<AnnouncingRpcPool> {
        self.rpc_pool.clone()
    }
}

/// Forwards the first received block announce to configured targets.
pub struct AnnounceForwarder {
    sync: RwLock<Option<Arc<SyncingService<Block>>>>,
    peers: Arc<AnnouncingPeersControl>,
}

impl AnnounceForwarder {
    pub fn new(peers: Arc<AnnouncingPeersControl>) -> Arc<Self> {
        Arc::new(Self {
            sync: RwLock::new(None),
            peers,
        })
    }

    pub fn set_sync(&self, sync: Arc<SyncingService<Block>>) {
        *self.sync.write().expect("poisoned") = Some(sync);
    }

    pub fn forward_first_announce(
        &self,
        header: &<Block as BlockT>::Header,
        data: &[u8],
        announcing_peer: Option<String>,
    ) {
        let targets = self.peers.targets();
        if targets.is_empty() {
            return;
        }

        let block_number = *header.number();
        let hash = header.hash();
        let delay_time_ms = current_delay_time_ms();

        match self.peers.mode() {
            AnnouncingMode::Sync => self.forward_sync(header, data, &targets, block_number, hash),
            AnnouncingMode::Rpc => self.forward_rpc(
                header,
                data,
                announcing_peer,
                delay_time_ms,
                &targets,
                block_number,
                hash,
            ),
        }
    }

    fn forward_sync(
        &self,
        header: &<Block as BlockT>::Header,
        data: &[u8],
        targets: &[String],
        block_number: u32,
        hash: <Block as BlockT>::Hash,
    ) {
        let Some(sync) = self.sync.read().expect("poisoned").clone() else {
            log::warn!(
                target: "bot::announce",
                "sync announce forwarder not wired yet",
            );
            return;
        };

        let peer_ids = self.peers.sync_peer_ids();
        if peer_ids.is_empty() {
            return;
        }

        let data = if data.is_empty() {
            None
        } else {
            Some(data.to_vec())
        };

        log::info!(
            target: "bot::announce",
            "forwarding first announce (sync) block #{block_number} hash={hash:?} to {} peer(s): {targets:?}",
            peer_ids.len(),
        );
        sync.forward_block_announce(header.clone(), data, true, peer_ids);
    }

    fn forward_rpc(
        &self,
        header: &<Block as BlockT>::Header,
        data: &[u8],
        announcing_peer: Option<String>,
        delay_time_ms: u64,
        targets: &[String],
        block_number: u32,
        hash: <Block as BlockT>::Hash,
    ) {
        let payload = ForwardedAnnouncePayload {
            header_number: block_number,
            hash: format!("{hash:?}"),
            parent_hash: format!("{:?}", header.parent_hash()),
            header_hex: format!("0x{}", hex::encode(header.encode())),
            data_hex: if data.is_empty() {
                None
            } else {
                Some(format!("0x{}", hex::encode(data)))
            },
            announcing_peer,
            delay_time_ms,
        };

        log::info!(
            target: "bot::announce",
            "forwarding first announce (rpc) block #{block_number} hash={hash:?} to {} endpoint(s): {targets:?}",
            targets.len(),
        );
        self.peers.rpc_pool().broadcast(&payload);
    }
}

fn decode_header_hex(header_hex: &str) -> Result<<Block as BlockT>::Header, String> {
    let bytes = subtensor_ipc::decode_hex(header_hex)?;
    <<Block as BlockT>::Header as Decode>::decode(&mut &bytes[..])
        .map_err(|e| format!("header decode: {e}"))
}

/// Applies a forwarded announce received over JSON-RPC on the receiving node.
pub struct AnnounceReceiveHandle<C> {
    client: Arc<C>,
    ipc: Option<Arc<IpcManager>>,
    propagation_tracker: Arc<PropagationTracker>,
    peer_tracker: Arc<PeerTracker>,
    peer_scoreboard: Arc<PeerScoreboard>,
    slot_state: Arc<SlotStateStore>,
    metrics_log: Arc<MetricsLogControl>,
    announce_index: Arc<AnnounceIndexTracker>,
}

impl<C> AnnounceReceiveHandle<C>
where
    C: HeaderBackend<Block> + Send + Sync + 'static,
{
    pub fn new(
        client: Arc<C>,
        ipc: Option<Arc<IpcManager>>,
        propagation_tracker: Arc<PropagationTracker>,
        peer_tracker: Arc<PeerTracker>,
        peer_scoreboard: Arc<PeerScoreboard>,
        slot_state: Arc<SlotStateStore>,
        metrics_log: Arc<MetricsLogControl>,
        announce_index: Arc<AnnounceIndexTracker>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            ipc,
            propagation_tracker,
            peer_tracker,
            peer_scoreboard,
            slot_state,
            metrics_log,
            announce_index,
        })
    }

    pub fn receive_forwarded(&self, payload: ForwardedAnnouncePayload) -> Result<bool, String> {
        let best_number = self.client.info().best_number;
        if payload.header_number != best_number.saturating_add(1) {
            return Err(format!(
                "forwarded announce #{} is not local best+1 (best #{best_number})",
                payload.header_number
            ));
        }

        let announce_index = self
            .announce_index
            .next_index(payload.header_number, best_number);
        let delay_time_ms = current_delay_time_ms();
        let announcing_peer = payload.announcing_peer.clone();
        let slot = decode_header_hex(&payload.header_hex)
            .ok()
            .and_then(|header| slot_from_digest(header.digest()));

        if announce_index == 1 {
            log::info!(
                target: "bot::announce",
                "forwarded first announce received: block #{} hash={} from={announcing_peer:?} delay_ms={delay_time_ms}",
                payload.header_number,
                payload.hash,
            );
            self.propagation_tracker.record_announce(
                payload.header_number,
                announcing_peer.clone(),
            );
            self.peer_tracker.record_announce(
                payload.header_number,
                std::iter::empty::<(String, u64, String)>(),
                announcing_peer.as_deref(),
            );

            if let Some(ref peer) = announcing_peer {
                self.peer_tracker
                    .record_announce_peer(payload.header_number, peer, delay_time_ms);
                self.peer_scoreboard.record_block_announce(
                    payload.header_number,
                    peer,
                    delay_time_ms,
                    true,
                );
                self.slot_state.record_announce(
                    payload.header_number,
                    peer,
                    delay_time_ms,
                    true,
                );
                log_peer_announce_timing(
                    &self.metrics_log,
                    payload.header_number,
                    peer,
                    announce_index,
                    delay_time_ms,
                );
            }

            if let Some(ipc) = &self.ipc {
                ipc.notify_header(IpcMessage::header(
                    payload.header_number,
                    payload.hash,
                    payload.parent_hash,
                    slot,
                    announcing_peer,
                    announce_index,
                    delay_time_ms,
                ));
            }
        } else {
            log::trace!(
                target: "bot::announce",
                "forwarded announce #{announce_index} for block #{} hash={} from={announcing_peer:?} (skipped IPC)",
                payload.header_number,
                payload.hash,
            );
        }

        Ok(announce_index == 1)
    }
}

impl<C> AnnounceReceiveRpc for AnnounceReceiveHandle<C>
where
    C: HeaderBackend<Block> + Send + Sync + 'static,
{
    fn receive_forwarded(&self, payload: ForwardedAnnouncePayload) -> Result<bool, String> {
        self.receive_forwarded(payload)
    }
}

impl<C> AnnounceReceiveRpc for Arc<AnnounceReceiveHandle<C>>
where
    C: HeaderBackend<Block> + Send + Sync + 'static,
{
    fn receive_forwarded(&self, payload: ForwardedAnnouncePayload) -> Result<bool, String> {
        self.as_ref().receive_forwarded(payload)
    }
}
