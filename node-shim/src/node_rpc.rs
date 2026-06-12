//! Node-side RPC: IPC, peer management, tx propagation.

use std::collections::HashMap;
use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorObjectOwned};
use sc_network::NetworkStatusProvider;
use subtensor_ipc::PeerManageMode;

use crate::announce_filter::AnnounceFilterControl;
use crate::announcing_peers::{
    AnnounceReceiveRpc, AnnouncingMode, AnnouncingPeersControl, AnnouncingPeersResult,
    ForwardedAnnouncePayload,
};
use crate::ipc::{BlockAnnounceIpcControl, IpcManagerConfig, MempoolIpcControl};
use crate::metrics_log::MetricsLogControl;
use crate::mempool::MempoolWatcherControl;
use crate::pool_log::PoolImportLogControl;
use crate::peer_manage::{
    ClearNormalPeersResult, ConnectFileResult, FindClosestPeersResult, PeerListEntry,
    PeerManageStatus, PeerManager, SetDisablePeersResult,
};
use crate::peer_scoreboard::{PeerScoreboard, PeerScoreboardExport};
use crate::slot_state::{SlotState, SlotStateExport, SlotStateStore};
use crate::propagation_tracker::{OwnPropagationRecord, PropagationTracker};
use crate::remote_nodes::{RemoteNodeEntry, RemoteNodesControl, RemoteNodesResult};
use crate::remote_submit::RemoteSubmitControl;
use crate::submit::{PreparedSubmitRequest, TxSubmitHandle};
use crate::transact::parse_propagation_peer_id;
use crate::tx_propagation::{PropagateMode, SetPropagationPeersResult, TxPropagationControl};
use crate::user_log::UserLogControl;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeStatus {
    pub socket_path: String,
    pub ipc_listening: bool,
    pub block_announce_ipc: bool,
    pub mempool_ipc: bool,
    pub mempool_watcher: bool,
    pub mempool_log: bool,
    pub propagate_mode: u8,
    pub propagate_mode_label: String,
    pub tx_propagation_first_reserved_node: bool,
    pub tx_propagation_max_peers: u32,
    pub tx_propagation_peers: Option<Vec<String>>,
    pub announce_filter_type: String,
    pub announce_filter_value: u64,
    pub announcing_peers: Vec<String>,
    pub announcing_mode: u8,
    pub announcing_mode_label: String,
    pub remote_nodes: Vec<RemoteNodeEntry>,
    pub remote_submit_enabled: bool,
    pub log_peer_announce_timing: bool,
    pub log_peer_rtt: bool,
    pub log_tx_inclusion_delay: bool,
    pub user_log: bool,
}

#[rpc(server)]
pub trait NodeControlApi {
    #[method(name = "node_status")]
    fn status(&self) -> RpcResult<NodeStatus>;

    #[method(name = "node_peerConnect")]
    fn peer_connect(&self, multiaddr: String) -> RpcResult<String>;

    #[method(name = "node_peerDisconnect")]
    fn peer_disconnect(&self, peer_id: String) -> RpcResult<bool>;

    #[method(name = "node_peerDisconnectAll")]
    fn peer_disconnect_all(&self) -> RpcResult<u32>;

    #[method(name = "node_peerConnectFromFile")]
    fn peer_connect_from_file(&self, path: String) -> RpcResult<ConnectFileResult>;

    #[method(name = "node_clearNormalPeers")]
    fn clear_normal_peers(&self) -> RpcResult<ClearNormalPeersResult>;

    #[method(name = "node_enableNormalPeer")]
    fn enable_normal_peer(&self) -> RpcResult<bool>;

    #[method(name = "node_disableNormalPeer")]
    fn disable_normal_peer(&self) -> RpcResult<ClearNormalPeersResult>;

    #[method(name = "node_enableLogPeer")]
    fn enable_log_peer(&self, path: Option<String>) -> RpcResult<bool>;

    #[method(name = "node_disableLogPeer")]
    fn disable_log_peer(&self) -> RpcResult<bool>;

    #[method(name = "node_peerSetMode")]
    fn peer_set_mode(&self, mode: u8) -> RpcResult<u8>;

    #[method(name = "node_peerSetCheckingTime")]
    fn peer_set_checking_time(&self, checking_ms: u64, sleep_ms: u64) -> RpcResult<bool>;

    #[method(name = "node_peerStatus")]
    fn peer_status(&self) -> RpcResult<PeerManageStatus>;

    /// Connected peers: full snapshot (addresses, sync state, scores, direction, flags).
    #[method(name = "node_peerList")]
    fn peer_list(&self) -> RpcResult<Vec<PeerListEntry>>;

    /// Replace the disabled peer set, persist to the configured disable-peers file, drop and ban matching peers.
    #[method(name = "node_setDisablePeers")]
    fn set_disable_peers(&self, peer_ids: Vec<String>) -> RpcResult<SetDisablePeersResult>;

    /// Replace disabled peers from a file (one base58 peer id per line), persist to the configured disable-peers file.
    #[method(name = "node_setDisablePeersFromFile")]
    fn set_disable_peers_from_file(&self, path: String) -> RpcResult<SetDisablePeersResult>;

    /// DHT closest peers to `peer_id` and their multiaddrs (`find_closest_peers`).
    #[method(name = "node_peerFindClosest")]
    fn peer_find_closest(&self, peer_id: String) -> RpcResult<FindClosestPeersResult>;

    /// Per-peer racing metrics and composite score (ranked highest first).
    #[method(name = "node_peerScores")]
    fn peer_scores(&self) -> RpcResult<PeerScoreboardExport>;

    /// Aggregated block announce summary for all 20 slot positions (`block_number % 20`).
    #[method(name = "node_slotState")]
    fn slot_state(&self) -> RpcResult<SlotStateExport>;

    /// Aggregated block announce summary for one slot position (0–19).
    #[method(name = "node_slotStateBySlot")]
    fn slot_state_by_slot(&self, slot: u32) -> RpcResult<SlotState>;

    #[method(name = "node_enableMempoolWatcher")]
    fn enable_mempool_watcher(&self) -> RpcResult<bool>;

    #[method(name = "node_disableMempoolWatcher")]
    fn disable_mempool_watcher(&self) -> RpcResult<bool>;

    /// Enable `bot::pool` import logging (each pool import + ready-queue order).
    #[method(name = "node_enableMempoolLog")]
    fn enable_mempool_log(&self) -> RpcResult<bool>;

    #[method(name = "node_disableMempoolLog")]
    fn disable_mempool_log(&self) -> RpcResult<bool>;

    #[method(name = "node_enableMempoolIpc")]
    fn enable_mempool_ipc(&self) -> RpcResult<bool>;

    #[method(name = "node_disableMempoolIpc")]
    fn disable_mempool_ipc(&self) -> RpcResult<bool>;

    #[method(name = "node_enableBlockAnnounceIpc")]
    fn enable_block_announce_ipc(&self) -> RpcResult<bool>;

    #[method(name = "node_disableBlockAnnounceIpc")]
    fn disable_block_announce_ipc(&self) -> RpcResult<bool>;

    /// Global announce filter for IPC header delivery (`count` or `delay_time`).
    #[method(name = "node_setAnnounceFilter")]
    fn set_announce_filter(&self, announce_type: String, value: u64) -> RpcResult<bool>;

    #[method(name = "node_getAnnounceFilter")]
    fn announce_filter(&self) -> RpcResult<(String, u64)>;

    /// Peers that receive forwarded first block announces.
    #[method(name = "node_announcingPeers")]
    fn announcing_peers(&self) -> RpcResult<Vec<String>>;

    #[method(name = "node_addAnnouncingPeer")]
    fn add_announcing_peer(&self, peer_id: String) -> RpcResult<AnnouncingPeersResult>;

    #[method(name = "node_removeAnnouncingPeer")]
    fn remove_announcing_peer(&self, peer_id: String) -> RpcResult<AnnouncingPeersResult>;

    #[method(name = "node_addAnnouncingPeersFromFile")]
    fn add_announcing_peers_from_file(&self, path: String) -> RpcResult<AnnouncingPeersResult>;

    #[method(name = "node_clearAnnouncingPeers")]
    fn clear_announcing_peers(&self) -> RpcResult<AnnouncingPeersResult>;

    /// `0` = libp2p sync, `1` = WS/HTTP JSON-RPC (recommended).
    #[method(name = "node_setAnnouncingMode")]
    fn set_announcing_mode(&self, mode: u8) -> RpcResult<u8>;

    #[method(name = "node_announcingMode")]
    fn announcing_mode(&self) -> RpcResult<u8>;

    /// Receive a forwarded first announce (rpc mode receiver on node B).
    #[method(name = "node_receiveForwardedAnnounce")]
    fn receive_forwarded_announce(
        &self,
        payload: ForwardedAnnouncePayload,
    ) -> RpcResult<bool>;

    /// Remote submit targets used by the node after IPC transactions (operator registry).
    #[method(name = "node_remoteNodes")]
    fn remote_nodes(&self) -> RpcResult<Vec<RemoteNodeEntry>>;

    #[method(name = "node_setRemoteNodes")]
    fn set_remote_nodes(&self, nodes: Vec<RemoteNodeEntry>) -> RpcResult<RemoteNodesResult>;

    #[method(name = "node_enableRemoteSubmit")]
    fn enable_remote_submit(&self) -> RpcResult<bool>;

    #[method(name = "node_disableRemoteSubmit")]
    fn disable_remote_submit(&self) -> RpcResult<bool>;

    #[method(name = "node_addRemoteNode")]
    fn add_remote_node(&self, node: RemoteNodeEntry) -> RpcResult<RemoteNodesResult>;

    #[method(name = "node_removeRemoteNode")]
    fn remove_remote_node(&self, name: String) -> RpcResult<RemoteNodesResult>;

    #[method(name = "node_clearRemoteNodes")]
    fn clear_remote_nodes(&self) -> RpcResult<RemoteNodesResult>;

    #[method(name = "node_setRemoteNodesFromFile")]
    fn set_remote_nodes_from_file(&self, path: String) -> RpcResult<RemoteNodesResult>;

    #[method(name = "node_enablePeerAnnounceTimingLog")]
    fn enable_peer_announce_timing_log(&self) -> RpcResult<bool>;

    #[method(name = "node_disablePeerAnnounceTimingLog")]
    fn disable_peer_announce_timing_log(&self) -> RpcResult<bool>;

    #[method(name = "node_enablePeerRttLog")]
    fn enable_peer_rtt_log(&self) -> RpcResult<bool>;

    #[method(name = "node_disablePeerRttLog")]
    fn disable_peer_rtt_log(&self) -> RpcResult<bool>;

    #[method(name = "node_enableTxInclusionDelayLog")]
    fn enable_tx_inclusion_delay_log(&self) -> RpcResult<bool>;

    #[method(name = "node_disableTxInclusionDelayLog")]
    fn disable_tx_inclusion_delay_log(&self) -> RpcResult<bool>;

    /// Show only custom `bot::*` logs (hide Substrate default logs).
    #[method(name = "node_enableUserLog")]
    fn enable_user_log(&self) -> RpcResult<bool>;

    /// Hide custom `bot::*` logs; restore Substrate default logs only.
    #[method(name = "node_disableUserLog")]
    fn disable_user_log(&self) -> RpcResult<bool>;

    #[method(name = "node_setPropagateMode")]
    fn set_propagate_mode(&self, mode: u8) -> RpcResult<u8>;

    #[method(name = "node_propagateMode")]
    fn propagate_mode(&self) -> RpcResult<u8>;

    #[method(name = "node_enableTxPropagationFirstReservedNode")]
    fn enable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool>;

    #[method(name = "node_disableTxPropagationFirstReservedNode")]
    fn disable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool>;

    #[method(name = "node_setTxPropagationMaxPeers")]
    fn set_tx_propagation_max_peers(&self, max: u32) -> RpcResult<bool>;

    #[method(name = "node_propagateToPeers")]
    fn propagate_to_peers(&self, peer_ids: Vec<String>) -> RpcResult<SetPropagationPeersResult>;

    #[method(name = "node_ownPropagationLatest")]
    fn own_propagation_latest(&self) -> RpcResult<Option<OwnPropagationRecord>>;

    #[method(name = "node_ownPropagationHistory")]
    fn own_propagation_history(&self, limit: Option<u32>) -> RpcResult<Vec<OwnPropagationRecord>>;

    /// Alias for [`Self::status`].
    #[method(name = "node_ipcStatus")]
    fn ipc_status(&self) -> RpcResult<NodeStatus>;

    /// Start or restart the Unix socket IPC listener on `path`.
    #[method(name = "node_startIpc")]
    fn start_ipc(&self, path: String) -> RpcResult<String>;

    /// Fast-path prepared extrinsic submit (same as IPC `extrinsic` field).
    ///
    /// Params: `extrinsic_hex`, optional `hash`, optional `peer_id`, optional `propagate_type`,
    /// optional `propagate_param`.
    #[method(name = "node_submitPreparedExtrinsic")]
    fn submit_prepared_extrinsic(
        &self,
        extrinsic_hex: String,
        hash: Option<String>,
        peer_id: Option<String>,
        propagate_type: Option<String>,
        propagate_param: Option<String>,
    ) -> RpcResult<String>;
}

pub struct NodeControlRpc {
    peer_manager: Arc<PeerManager>,
    peer_scoreboard: Arc<PeerScoreboard>,
    slot_state: Arc<SlotStateStore>,
    propagation_tracker: Arc<PropagationTracker>,
    mempool_watcher: Arc<MempoolWatcherControl>,
    pool_import_log: Arc<PoolImportLogControl>,
    block_announce_ipc: Arc<BlockAnnounceIpcControl>,
    announce_filter: Arc<AnnounceFilterControl>,
    mempool_ipc: Arc<MempoolIpcControl>,
    ipc_config: IpcManagerConfig,
    metrics_log: Arc<MetricsLogControl>,
    user_log: Arc<UserLogControl>,
    tx_propagation: Arc<TxPropagationControl>,
    announcing_peers: Arc<AnnouncingPeersControl>,
    remote_nodes: Arc<RemoteNodesControl>,
    remote_submit: Arc<RemoteSubmitControl>,
    announce_receive: Arc<dyn AnnounceReceiveRpc + Send + Sync>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    tx_submit: Arc<TxSubmitHandle>,
}

impl NodeControlRpc {
    pub fn new(
        peer_manager: Arc<PeerManager>,
        peer_scoreboard: Arc<PeerScoreboard>,
        slot_state: Arc<SlotStateStore>,
        propagation_tracker: Arc<PropagationTracker>,
        mempool_watcher: Arc<MempoolWatcherControl>,
        pool_import_log: Arc<PoolImportLogControl>,
        block_announce_ipc: Arc<BlockAnnounceIpcControl>,
        announce_filter: Arc<AnnounceFilterControl>,
        mempool_ipc: Arc<MempoolIpcControl>,
        ipc_config: IpcManagerConfig,
        metrics_log: Arc<MetricsLogControl>,
        user_log: Arc<UserLogControl>,
        tx_propagation: Arc<TxPropagationControl>,
        announcing_peers: Arc<AnnouncingPeersControl>,
        remote_nodes: Arc<RemoteNodesControl>,
        remote_submit: Arc<RemoteSubmitControl>,
        announce_receive: Arc<dyn AnnounceReceiveRpc + Send + Sync>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
        tx_submit: Arc<TxSubmitHandle>,
    ) -> Self {
        Self {
            peer_manager,
            peer_scoreboard,
            slot_state,
            propagation_tracker,
            mempool_watcher,
            pool_import_log,
            block_announce_ipc,
            announce_filter,
            mempool_ipc,
            ipc_config,
            metrics_log,
            user_log,
            tx_propagation,
            announcing_peers,
            remote_nodes,
            remote_submit,
            announce_receive,
            network,
            tx_submit,
        }
    }

    fn peer_addrs(&self) -> HashMap<String, String> {
        let network = Arc::clone(&self.network);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                crate::peers::connected_peer_addresses(network.as_ref()).await
            })
        })
    }

    fn node_status(&self) -> NodeStatus {
        let mode = self.tx_propagation.propagate_mode();
        let (announce_filter_type, announce_filter_value) = self.announce_filter.describe();
        NodeStatus {
            socket_path: self.ipc_config.socket_path(),
            ipc_listening: self.ipc_config.is_listening(),
            block_announce_ipc: self.block_announce_ipc.is_enabled(),
            mempool_ipc: self.mempool_ipc.is_enabled(),
            mempool_watcher: self.mempool_watcher.is_running(),
            mempool_log: self.pool_import_log.is_enabled(),
            propagate_mode: mode.as_u8(),
            propagate_mode_label: mode.label().into(),
            tx_propagation_first_reserved_node: self.tx_propagation.first_reserved_node(),
            tx_propagation_max_peers: self.tx_propagation.max_propagation_peers(),
            tx_propagation_peers: self.tx_propagation.propagation_allowlist_base58(),
            announce_filter_type,
            announce_filter_value,
            announcing_peers: self.announcing_peers.targets(),
            announcing_mode: self.announcing_peers.mode().as_u8(),
            announcing_mode_label: self.announcing_peers.mode().label().into(),
            remote_nodes: self.remote_nodes.list(),
            remote_submit_enabled: self.remote_submit.is_enabled(),
            log_peer_announce_timing: self.metrics_log.peer_announce_timing(),
            log_peer_rtt: self.metrics_log.peer_rtt(),
            log_tx_inclusion_delay: self.metrics_log.tx_inclusion_delay(),
            user_log: self.user_log.is_enabled(),
        }
    }
}

impl NodeControlApiServer for NodeControlRpc {
    fn status(&self) -> RpcResult<NodeStatus> {
        Ok(self.node_status())
    }

    fn ipc_status(&self) -> RpcResult<NodeStatus> {
        self.status()
    }

    fn start_ipc(&self, path: String) -> RpcResult<String> {
        self.ipc_config
            .start_ipc(path)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn peer_connect(&self, multiaddr: String) -> RpcResult<String> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pm.connect(&multiaddr).await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_disconnect(&self, peer_id: String) -> RpcResult<bool> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                pm.disconnect(&peer_id).await.map(|_| true)
            })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_disconnect_all(&self) -> RpcResult<u32> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pm.disconnect_all().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_connect_from_file(&self, path: String) -> RpcResult<ConnectFileResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { pm.connect_with_file(&path).await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn clear_normal_peers(&self) -> RpcResult<ClearNormalPeersResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pm.clear_normal_peers().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn enable_normal_peer(&self) -> RpcResult<bool> {
        self.peer_manager.enable_normal_peers();
        Ok(true)
    }

    fn disable_normal_peer(&self) -> RpcResult<ClearNormalPeersResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { pm.disable_normal_peers().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn enable_log_peer(&self, path: Option<String>) -> RpcResult<bool> {
        self.peer_manager.enable_log_peer(path);
        Ok(true)
    }

    fn disable_log_peer(&self) -> RpcResult<bool> {
        self.peer_manager.disable_log_peer();
        Ok(true)
    }

    fn peer_set_mode(&self, mode: u8) -> RpcResult<u8> {
        let mode = PeerManageMode::from_u8(mode).ok_or_else(|| {
            ErrorObjectOwned::owned(
                -32602,
                "mode must be 0 (only_custom), 1 (both), or 2 (only_system)",
                None::<()>,
            )
        })?;
        self.peer_manager.set_mode(mode);
        Ok(mode.as_u8())
    }

    fn peer_set_checking_time(&self, checking_ms: u64, sleep_ms: u64) -> RpcResult<bool> {
        self.peer_manager.set_checking_time(checking_ms, sleep_ms);
        Ok(true)
    }

    fn peer_status(&self) -> RpcResult<PeerManageStatus> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pm.get_status().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_list(&self) -> RpcResult<Vec<PeerListEntry>> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { pm.get_peer_list().await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn set_disable_peers(&self, peer_ids: Vec<String>) -> RpcResult<SetDisablePeersResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { pm.set_disable_peers(peer_ids).await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn set_disable_peers_from_file(&self, path: String) -> RpcResult<SetDisablePeersResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { pm.set_disable_peers_from_file(&path).await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_find_closest(&self, peer_id: String) -> RpcResult<FindClosestPeersResult> {
        let pm = Arc::clone(&self.peer_manager);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { pm.find_closest_peers(&peer_id).await })
        })
        .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn peer_scores(&self) -> RpcResult<PeerScoreboardExport> {
        let pm = Arc::clone(&self.peer_manager);
        let scoreboard = Arc::clone(&self.peer_scoreboard);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let connected = pm.connected_peer_ids().await;
                Ok(scoreboard.export_ranked(connected))
            })
        })
    }

    fn slot_state(&self) -> RpcResult<SlotStateExport> {
        Ok(self.slot_state.export())
    }

    fn slot_state_by_slot(&self, slot: u32) -> RpcResult<SlotState> {
        self.slot_state.slot(slot).ok_or_else(|| {
            ErrorObjectOwned::owned(
                -32602,
                format!("slot must be 0..{}", crate::slot_state::SLOT_COUNT - 1),
                None::<()>,
            )
        })
    }

    fn enable_mempool_watcher(&self) -> RpcResult<bool> {
        self.mempool_watcher.start();
        Ok(true)
    }

    fn disable_mempool_watcher(&self) -> RpcResult<bool> {
        self.mempool_watcher.stop();
        Ok(true)
    }

    fn enable_mempool_log(&self) -> RpcResult<bool> {
        self.pool_import_log.enable();
        Ok(true)
    }

    fn disable_mempool_log(&self) -> RpcResult<bool> {
        self.pool_import_log.disable();
        Ok(true)
    }

    fn enable_mempool_ipc(&self) -> RpcResult<bool> {
        self.mempool_ipc.enable();
        Ok(true)
    }

    fn disable_mempool_ipc(&self) -> RpcResult<bool> {
        self.mempool_ipc.disable();
        Ok(true)
    }

    fn enable_block_announce_ipc(&self) -> RpcResult<bool> {
        self.block_announce_ipc.enable();
        Ok(true)
    }

    fn disable_block_announce_ipc(&self) -> RpcResult<bool> {
        self.block_announce_ipc.disable();
        Ok(true)
    }

    fn set_announce_filter(&self, announce_type: String, value: u64) -> RpcResult<bool> {
        self.announce_filter
            .set(&announce_type, value)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))?;
        Ok(true)
    }

    fn announce_filter(&self) -> RpcResult<(String, u64)> {
        Ok(self.announce_filter.describe())
    }

    fn announcing_peers(&self) -> RpcResult<Vec<String>> {
        Ok(self.announcing_peers.targets())
    }

    fn add_announcing_peer(&self, peer_id: String) -> RpcResult<AnnouncingPeersResult> {
        self.announcing_peers
            .add(&peer_id)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn remove_announcing_peer(&self, peer_id: String) -> RpcResult<AnnouncingPeersResult> {
        self.announcing_peers
            .remove(&peer_id)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn add_announcing_peers_from_file(&self, path: String) -> RpcResult<AnnouncingPeersResult> {
        self.announcing_peers
            .add_from_file(path.trim())
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn clear_announcing_peers(&self) -> RpcResult<AnnouncingPeersResult> {
        self.announcing_peers
            .clear_all()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn set_announcing_mode(&self, mode: u8) -> RpcResult<u8> {
        let mode = AnnouncingMode::from_u8(mode).ok_or_else(|| {
            ErrorObjectOwned::owned(
                -32602,
                "mode must be 0 (sync) or 1 (rpc)",
                None::<()>,
            )
        })?;
        self.announcing_peers
            .set_mode(mode)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))?;
        Ok(mode.as_u8())
    }

    fn announcing_mode(&self) -> RpcResult<u8> {
        Ok(self.announcing_peers.mode().as_u8())
    }

    fn receive_forwarded_announce(
        &self,
        payload: ForwardedAnnouncePayload,
    ) -> RpcResult<bool> {
        self.announce_receive
            .receive_forwarded(payload)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn remote_nodes(&self) -> RpcResult<Vec<RemoteNodeEntry>> {
        Ok(self.remote_nodes.list())
    }

    fn set_remote_nodes(&self, nodes: Vec<RemoteNodeEntry>) -> RpcResult<RemoteNodesResult> {
        self.remote_nodes
            .set_all(nodes)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn add_remote_node(&self, node: RemoteNodeEntry) -> RpcResult<RemoteNodesResult> {
        self.remote_nodes
            .add(node)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn remove_remote_node(&self, name: String) -> RpcResult<RemoteNodesResult> {
        self.remote_nodes
            .remove(&name)
            .map_err(|e| ErrorObjectOwned::owned(-32602, e, None::<()>))
    }

    fn clear_remote_nodes(&self) -> RpcResult<RemoteNodesResult> {
        self.remote_nodes
            .clear()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn set_remote_nodes_from_file(&self, path: String) -> RpcResult<RemoteNodesResult> {
        self.remote_nodes
            .set_from_file(path.trim())
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }

    fn enable_remote_submit(&self) -> RpcResult<bool> {
        self.remote_submit.enable();
        Ok(true)
    }

    fn disable_remote_submit(&self) -> RpcResult<bool> {
        self.remote_submit.disable();
        Ok(true)
    }

    fn enable_peer_announce_timing_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_peer_announce_timing(true);
        Ok(true)
    }

    fn disable_peer_announce_timing_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_peer_announce_timing(false);
        Ok(true)
    }

    fn enable_peer_rtt_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_peer_rtt(true);
        Ok(true)
    }

    fn disable_peer_rtt_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_peer_rtt(false);
        Ok(true)
    }

    fn enable_tx_inclusion_delay_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_tx_inclusion_delay(true);
        Ok(true)
    }

    fn disable_tx_inclusion_delay_log(&self) -> RpcResult<bool> {
        self.metrics_log.set_tx_inclusion_delay(false);
        Ok(true)
    }

    fn enable_user_log(&self) -> RpcResult<bool> {
        self.user_log
            .apply_user_logs()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))?;
        Ok(true)
    }

    fn disable_user_log(&self) -> RpcResult<bool> {
        self.user_log
            .apply_system_logs()
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))?;
        Ok(true)
    }

    fn set_propagate_mode(&self, mode: u8) -> RpcResult<u8> {
        let mode = PropagateMode::from_u8(mode).ok_or_else(|| {
            ErrorObjectOwned::owned(
                -32602,
                "mode must be 0 (normal), 1 (announce), or 2 (parallel)",
                None::<()>,
            )
        })?;
        self.tx_propagation.set_propagate_mode(mode);
        Ok(mode.as_u8())
    }

    fn propagate_mode(&self) -> RpcResult<u8> {
        Ok(self.tx_propagation.propagate_mode().as_u8())
    }

    fn enable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool> {
        self.tx_propagation.enable_first_reserved_node();
        Ok(true)
    }

    fn disable_tx_propagation_first_reserved_node(&self) -> RpcResult<bool> {
        self.tx_propagation.disable_first_reserved_node();
        Ok(true)
    }

    fn set_tx_propagation_max_peers(&self, max: u32) -> RpcResult<bool> {
        self.tx_propagation.set_max_propagation_peers(max);
        Ok(true)
    }

    fn propagate_to_peers(&self, peer_ids: Vec<String>) -> RpcResult<SetPropagationPeersResult> {
        let result = self.tx_propagation.set_propagation_allowlist(
            peer_ids,
            parse_propagation_peer_id,
        );

        if !result.enabled && !result.invalid_peer_ids.is_empty() && result.peers.is_empty() {
            return Err(ErrorObjectOwned::owned(
                -32602,
                format!("no valid peer ids: {:?}", result.invalid_peer_ids),
                Some(result),
            ));
        }

        Ok(result)
    }

    fn own_propagation_latest(&self) -> RpcResult<Option<OwnPropagationRecord>> {
        let addrs = self.peer_addrs();
        Ok(self
            .propagation_tracker
            .latest()
            .map(|r| PropagationTracker::enrich_record(r, &addrs)))
    }

    fn own_propagation_history(&self, limit: Option<u32>) -> RpcResult<Vec<OwnPropagationRecord>> {
        let limit = limit.unwrap_or(20).clamp(1, 100) as usize;
        let addrs = self.peer_addrs();
        Ok(self
            .propagation_tracker
            .history(limit)
            .into_iter()
            .map(|r| PropagationTracker::enrich_record(r, &addrs))
            .collect())
    }

    fn submit_prepared_extrinsic(
        &self,
        extrinsic_hex: String,
        hash: Option<String>,
        peer_id: Option<String>,
        propagate_type: Option<String>,
        propagate_param: Option<String>,
    ) -> RpcResult<String> {
        let request = PreparedSubmitRequest {
            hash: hash.unwrap_or_default(),
            extrinsic: Some(extrinsic_hex),
            propagate_type,
            propagate_param,
            peer_id,
        };
        self.tx_submit
            .submit(request)
            .map_err(|e| ErrorObjectOwned::owned(-32000, e, None::<()>))
    }
}
