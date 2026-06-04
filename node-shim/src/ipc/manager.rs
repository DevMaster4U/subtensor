//! Node-side IPC server: forwards header / mempool events to bot clients
//! and receives transactions to submit + propagate.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use codec::Decode;
use futures::FutureExt;
use node_subtensor_runtime::opaque::Block;
use sc_service::TaskManager;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_core::H256;
use sp_runtime::OpaqueExtrinsic;
use subtensor_ipc::{decode_frame, encode_frame, IpcMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch, Mutex};

use super::client_config::ClientConfig;
use crate::announce_filter::AnnounceFilterControl;
use crate::metrics_log::TxInclusionTracker;
use crate::peer_manage::PeerManager;
use crate::propagation_tracker::PropagationTracker;
use crate::transact::TxPropagator;
use crate::tx_propagation::{PropagateMode, RankFunction, TxPropagationControl, TxPropagationRequest};

/// Runtime toggle for forwarding block announce headers over IPC.
pub struct BlockAnnounceIpcControl {
    enabled: AtomicBool,
}

impl Default for BlockAnnounceIpcControl {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
        }
    }
}

impl BlockAnnounceIpcControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
        log::info!(target: "bot::ipc", "header IPC forwarding enabled");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        log::info!(target: "bot::ipc", "header IPC forwarding disabled");
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
}

/// Runtime toggle for forwarding mempool watcher events over IPC.
pub struct MempoolIpcControl {
    enabled: AtomicBool,
}

impl Default for MempoolIpcControl {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
        }
    }
}

impl MempoolIpcControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
        log::info!(target: "bot::ipc", "mempool IPC forwarding enabled");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        log::info!(target: "bot::ipc", "mempool IPC forwarding disabled");
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
}

struct IpcManagerConfigInner {
    socket_path: RwLock<String>,
    /// Incremented on each `start_ipc`; the server rebinds when this changes.
    generation: AtomicU64,
    generation_notify: watch::Sender<u64>,
    bound: AtomicBool,
}

/// Shared IPC socket path; bind is deferred until [`Self::start_ipc`] is called.
#[derive(Clone)]
pub struct IpcManagerConfig {
    inner: Arc<IpcManagerConfigInner>,
}

impl Default for IpcManagerConfig {
    fn default() -> Self {
        let (generation_notify, _) = watch::channel(0u64);
        Self {
            inner: Arc::new(IpcManagerConfigInner {
                socket_path: RwLock::new(String::new()),
                generation: AtomicU64::new(0),
                generation_notify,
                bound: AtomicBool::new(false),
            }),
        }
    }
}

impl IpcManagerConfig {
    pub fn socket_path(&self) -> String {
        self.inner
            .socket_path
            .read()
            .expect("ipc socket_path lock poisoned")
            .clone()
    }

    pub fn is_listening(&self) -> bool {
        self.inner.bound.load(Ordering::SeqCst)
    }

    pub fn is_started(&self) -> bool {
        self.inner.generation.load(Ordering::SeqCst) > 0
    }

    /// Start or restart the IPC listener on `path` (creates parent dirs if needed).
    pub fn start_ipc(&self, path: String) -> Result<String, String> {
        let path = path.trim().to_string();
        if path.is_empty() {
            return Err("ipc path must not be empty".into());
        }
        ensure_socket_parent(&path)?;
        {
            let mut guard = self
                .inner
                .socket_path
                .write()
                .expect("ipc socket_path lock poisoned");
            *guard = path.clone();
        }
        let generation = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.bound.store(false, Ordering::SeqCst);
        let _ = self.inner.generation_notify.send(generation);
        Ok(path)
    }

    fn set_bound(&self, bound: bool) {
        self.inner.bound.store(bound, Ordering::SeqCst);
    }

    fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::SeqCst)
    }

    fn generation_notify(&self) -> watch::Receiver<u64> {
        self.inner.generation_notify.subscribe()
    }
}

fn ensure_socket_parent(path: &str) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!("create socket directory {}: {e}", parent.display())
            })?;
        }
    }
    Ok(())
}

fn prepare_and_bind(path: &str) -> Result<UnixListener, String> {
    ensure_socket_parent(path)?;
    if Path::new(path).exists() {
        std::fs::remove_file(path).map_err(|e| format!("remove old socket {path}: {e}"))?;
    }
    UnixListener::bind(path).map_err(|e| format!("bind unix socket {path}: {e}"))
}

struct ClientSession {
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    config: ClientConfig,
}

/// Shared handle used by block announce hook and mempool watcher.
#[derive(Clone)]
pub struct IpcManager {
    block_announce: Arc<BlockAnnounceIpcControl>,
    announce_filter: Arc<AnnounceFilterControl>,
    mempool: Arc<MempoolIpcControl>,
    clients: Arc<Mutex<HashMap<u64, ClientSession>>>,
    peer_manager: Arc<Mutex<Option<Arc<PeerManager>>>>,
    tx_propagation: Arc<Mutex<Option<Arc<TxPropagationControl>>>>,
    tx_propagator: Arc<Mutex<Option<TxPropagator>>>,
    propagation_tracker: Arc<Mutex<Option<Arc<PropagationTracker>>>>,
    tx_inclusion_tracker: Arc<Mutex<Option<Arc<TxInclusionTracker>>>>,
}

impl IpcManager {
    pub fn new(
        block_announce: Arc<BlockAnnounceIpcControl>,
        announce_filter: Arc<AnnounceFilterControl>,
        mempool: Arc<MempoolIpcControl>,
    ) -> Self {
        Self {
            block_announce,
            announce_filter,
            mempool,
            clients: Arc::new(Mutex::new(HashMap::new())),
            peer_manager: Arc::new(Mutex::new(None)),
            tx_propagation: Arc::new(Mutex::new(None)),
            tx_propagator: Arc::new(Mutex::new(None)),
            propagation_tracker: Arc::new(Mutex::new(None)),
            tx_inclusion_tracker: Arc::new(Mutex::new(None)),
        }
    }

    pub fn block_announce_control(&self) -> Arc<BlockAnnounceIpcControl> {
        self.block_announce.clone()
    }

    pub fn announce_filter_control(&self) -> Arc<AnnounceFilterControl> {
        self.announce_filter.clone()
    }

    pub fn mempool_control(&self) -> Arc<MempoolIpcControl> {
        self.mempool.clone()
    }

    pub async fn set_peer_manager(&self, peer_manager: Arc<PeerManager>) {
        *self.peer_manager.lock().await = Some(peer_manager);
    }

    pub async fn set_tx_controls(
        &self,
        tx_propagation: Arc<TxPropagationControl>,
        tx_propagator: TxPropagator,
        propagation_tracker: Arc<PropagationTracker>,
        tx_inclusion_tracker: Arc<TxInclusionTracker>,
    ) {
        *self.tx_propagation.lock().await = Some(tx_propagation);
        *self.tx_propagator.lock().await = Some(tx_propagator);
        *self.propagation_tracker.lock().await = Some(propagation_tracker);
        *self.tx_inclusion_tracker.lock().await = Some(tx_inclusion_tracker);
    }

    async fn broadcast_filtered<F>(&self, message: &IpcMessage, filter: F)
    where
        F: Fn(&ClientConfig, &IpcMessage) -> bool,
    {
        let frame = match encode_frame(message) {
            Ok(frame) => frame,
            Err(e) => {
                log::warn!(target: "bot::ipc", "encode frame: {e}");
                return;
            }
        };

        let mut clients = self.clients.lock().await;
        clients.retain(|_, session| {
            if filter(&session.config, message) {
                session.outbound.send(frame.clone()).is_ok()
            } else {
                true
            }
        });
    }

    pub fn notify_header(&self, msg: IpcMessage) {
        let IpcMessage::Header {
            header_number,
            hash,
            parent_hash,
            slot,
            announcing_peer,
            announce_index,
            delay_time_ms,
        } = msg
        else {
            log::warn!(target: "bot::ipc", "notify_header called with non-header message");
            return;
        };
        if !self.block_announce.is_enabled() {
            return;
        }
        log::info!(
            target: "bot::ipc",
            "notify bot: header #{header_number} hash={hash} parent={parent_hash} slot={slot:?} from={announcing_peer:?} idx={announce_index} delay_ms={delay_time_ms}",
        );
        let message = IpcMessage::Header {
            header_number,
            hash,
            parent_hash,
            slot,
            announcing_peer,
            announce_index,
            delay_time_ms,
        };
        let ipc = self.clone();
        let filter = self.announce_filter.clone();
        tokio::spawn(async move {
            ipc.broadcast_filtered(&message, |_config, msg| {
                let IpcMessage::Header {
                    announce_index,
                    delay_time_ms,
                    ..
                } = msg
                else {
                    return false;
                };
                filter.matches(*announce_index, *delay_time_ms)
            })
            .await;
        });
    }

    pub fn notify_mempool(&self, info: String) {
        if !self.mempool.is_enabled() {
            return;
        }
        let tx_hash = serde_json::from_str::<serde_json::Value>(&info)
            .ok()
            .and_then(|v| v.get("tx_hash").and_then(|h| h.as_str()).map(str::to_string))
            .unwrap_or_else(|| "?".into());
        log::info!(
            target: "bot::ipc",
            "notify bot: mempool tx_hash={tx_hash}",
        );
        let message = IpcMessage::mempool(info);
        let ipc = self.clone();
        tokio::spawn(async move {
            ipc.broadcast_filtered(&message, |config, msg| {
                matches!(msg, IpcMessage::Mempool { .. }) && config.require_mempool
            })
            .await;
        });
    }

    pub fn notify_find_peer(&self, peer_id: String, multiaddr: String) {
        log::info!(
            target: "bot::ipc",
            "notify bot: find_peer peer_id={peer_id} multiaddr={multiaddr}",
        );
        let message = IpcMessage::find_peer(peer_id, multiaddr);
        let ipc = self.clone();
        tokio::spawn(async move {
            ipc.broadcast_filtered(&message, |config, msg| {
                matches!(msg, IpcMessage::FindPeer { .. }) && config.require_peer_find
            })
            .await;
        });
    }

    pub fn start<P>(
        self: Arc<Self>,
        task_manager: &TaskManager,
        pool: Arc<P>,
        config: IpcManagerConfig,
        best_hash: Arc<dyn Fn() -> H256 + Send + Sync>,
    ) where
        P: TransactionPool<Block = Block, Hash = H256>
            + LocalTransactionPool<Block = Block, Hash = H256>
            + 'static,
    {
        task_manager.spawn_handle().spawn("bot-ipc-manager", None, {
            async move {
                run_ipc_server(self, pool, config, best_hash).await;
            }
            .boxed()
        });
    }
}

async fn run_ipc_server<P>(
    ipc: Arc<IpcManager>,
    pool: Arc<P>,
    config: IpcManagerConfig,
    best_hash: Arc<dyn Fn() -> H256 + Send + Sync>,
) where
    P: TransactionPool<Block = Block, Hash = H256>
        + LocalTransactionPool<Block = Block, Hash = H256>
        + 'static,
{
    let (incoming_tx, mut incoming_rx) =
        mpsc::unbounded_channel::<(u64, IpcMessage, mpsc::UnboundedSender<Result<(), String>>)>();
    let mut next_id = 0u64;
    let mut gen_rx = config.generation_notify();
    let mut active_generation = 0u64;

    'rebind: loop {
        while active_generation == 0 {
            if gen_rx.changed().await.is_err() {
                return;
            }
            active_generation = *gen_rx.borrow();
        }

        let path = config.socket_path();
        config.set_bound(false);

        let listener = loop {
            match prepare_and_bind(&path) {
                Ok(listener) => break listener,
                Err(e) => {
                    log::error!(
                        target: "bot::ipc",
                        "{e}; retrying in 3s",
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    if config.generation() != active_generation {
                        continue 'rebind;
                    }
                }
            }
        };
        if config.generation() != active_generation {
            continue 'rebind;
        }

        config.set_bound(true);
        log::info!(target: "bot::ipc", "IPC listening on {path}");
        let bound_generation = active_generation;

        loop {
            tokio::select! {
                changed = gen_rx.changed() => {
                    if changed.is_err() {
                        config.set_bound(false);
                        return;
                    }
                    if *gen_rx.borrow() != bound_generation {
                        log::info!(target: "bot::ipc", "IPC restart requested, rebinding");
                        config.set_bound(false);
                        active_generation = *gen_rx.borrow();
                        break;
                    }
                }
                accept = listener.accept() => {
                    let (stream, _) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(target: "bot::ipc", "accept error: {e}");
                            continue;
                        }
                    };
                    next_id += 1;
                    let id = next_id;
                    let (client_tx, client_rx) = mpsc::unbounded_channel();
                    {
                        let mut clients = ipc.clients.lock().await;
                        clients.insert(id, ClientSession {
                            outbound: client_tx,
                            config: ClientConfig::default(),
                        });
                    }
                    let incoming_tx_c = incoming_tx.clone();
                    let ipc_c = ipc.clone();
                    tokio::spawn(handle_client(id, stream, client_rx, incoming_tx_c, ipc_c));
                    log::info!(target: "bot::ipc", "client {id} connected");
                }
                msg = incoming_rx.recv() => {
                    if let Some((client_id, message, reply)) = msg {
                        let result = handle_incoming(client_id, message, &ipc, &pool, &best_hash).await;
                        let _ = reply.send(result);
                    }
                }
            }
        }
    }
}

async fn handle_client(
    id: u64,
    stream: UnixStream,
    mut outbound: mpsc::UnboundedReceiver<Vec<u8>>,
    incoming_tx: mpsc::UnboundedSender<(u64, IpcMessage, mpsc::UnboundedSender<Result<(), String>>)>,
    ipc: Arc<IpcManager>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let write_task = tokio::spawn(async move {
        while let Some(frame) = write_rx.recv().await {
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
    });

    let write_tx_c = write_tx.clone();
    tokio::spawn(async move {
        while let Some(frame) = outbound.recv().await {
            if write_tx_c.send(frame).is_err() {
                break;
            }
        }
    });

    let mut buf = Vec::new();
    let mut scratch = [0u8; 4096];

    loop {
        match reader.read(&mut scratch).await {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&scratch[..n]);
                loop {
                    match decode_frame::<IpcMessage>(&buf) {
                        Ok(Some((message, consumed))) => {
                            buf.drain(..consumed);
                            let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
                            if incoming_tx.send((id, message, reply_tx)).is_err() {
                                break;
                            }
                            if let Some(result) = reply_rx.recv().await {
                                if let Err(e) = result {
                                    log::warn!(target: "bot::ipc", "client {id} message failed: {e}");
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            log::warn!(target: "bot::ipc", "client {id} decode error: {e}");
                            buf.clear();
                            break;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }

    ipc.clients.lock().await.remove(&id);
    write_task.abort();
    log::info!(target: "bot::ipc", "client {id} disconnected");
}

async fn handle_incoming<P>(
    client_id: u64,
    message: IpcMessage,
    ipc: &Arc<IpcManager>,
    pool: &Arc<P>,
    best_hash: &Arc<dyn Fn() -> H256 + Send + Sync>,
) -> Result<(), String>
where
    P: TransactionPool<Block = Block, Hash = H256>
        + LocalTransactionPool<Block = Block, Hash = H256>
        + 'static,
{
    match message {
        IpcMessage::SetAnnounce { .. } => Err(
            "set_announce is deprecated; use node_setAnnounceFilter RPC".into(),
        ),
        IpcMessage::SetRequireMempool { enabled } => {
            let mut clients = ipc.clients.lock().await;
            let session = clients
                .get_mut(&client_id)
                .ok_or_else(|| "client disconnected".to_string())?;
            session.config.require_mempool = enabled;
            log::info!(
                target: "bot::ipc",
                "client {client_id} require_mempool={enabled}",
            );
            Ok(())
        }
        IpcMessage::SetRequirePeerFind { enabled } => {
            let mut clients = ipc.clients.lock().await;
            let session = clients
                .get_mut(&client_id)
                .ok_or_else(|| "client disconnected".to_string())?;
            session.config.require_peer_find = enabled;
            log::info!(
                target: "bot::ipc",
                "client {client_id} require_peer_find={enabled}",
            );
            Ok(())
        }
        IpcMessage::Transaction {
            hash,
            extrinsic,
            propagate_type,
            propagate_param,
        } => {
            let tx_control = ipc.tx_propagation.lock().await.clone();
            let tx_propagator = ipc.tx_propagator.lock().await.clone();
            let tracker = ipc.propagation_tracker.lock().await.clone();
            let inclusion = ipc.tx_inclusion_tracker.lock().await.clone();

            if let Some(tx_control) = tx_control {
                if let Some(request) = parse_propagation_request(propagate_type, propagate_param) {
                    tx_control.set_pending_request(request);
                }
                let result = handle_transaction(
                    hash,
                    extrinsic,
                    pool,
                    best_hash,
                    tx_propagator.as_ref(),
                    tracker.as_ref(),
                    inclusion.as_ref(),
                )
                .await;
                result
            } else {
                handle_transaction(hash, extrinsic, pool, best_hash, None, None, None).await
            }
        }
        IpcMessage::Header { .. } | IpcMessage::Mempool { .. } | IpcMessage::FindPeer { .. } => {
            Err("header, mempool, and find_peer messages are node → bot only".into())
        }
    }
}

fn parse_propagation_request(
    propagate_type: Option<String>,
    propagate_param: Option<String>,
) -> Option<TxPropagationRequest> {
    let propagate_type = propagate_type?;
    match propagate_type.as_str() {
        "normal" => {
            let rank_name = propagate_param.as_deref().unwrap_or("first_announce_hit_count");
            let rank_function = RankFunction::from_name(rank_name)?;
            Some(TxPropagationRequest {
                mode: PropagateMode::Normal,
                rank_function: Some(rank_function),
                announce_count: None,
            })
        }
        "announce" => {
            let count = propagate_param
                .as_deref()
                .unwrap_or("1")
                .parse::<u32>()
                .ok()?;
            Some(TxPropagationRequest {
                mode: PropagateMode::Parallel,
                rank_function: None,
                announce_count: Some(count),
            })
        }
        "parallel" | "parrel" => Some(TxPropagationRequest {
            mode: PropagateMode::Parallel,
            rank_function: None,
            announce_count: None,
        }),
        _ => None,
    }
}

async fn handle_transaction<P>(
    hash: String,
    extrinsic: Option<String>,
    pool: &Arc<P>,
    best_hash: &Arc<dyn Fn() -> H256 + Send + Sync>,
    tx_propagator: Option<&TxPropagator>,
    _propagation_tracker: Option<&Arc<PropagationTracker>>,
    tx_inclusion_tracker: Option<&Arc<TxInclusionTracker>>,
) -> Result<(), String>
where
    P: TransactionPool<Block = Block, Hash = H256>
        + LocalTransactionPool<Block = Block, Hash = H256>
        + 'static,
{
    // Fast path: client already validated SCALE; node hex-decodes inner payload only.
    if let Some(inner_hex) = extrinsic {
        let inner = subtensor_ipc::decode_hex(&inner_hex)?;
        if inner.is_empty() {
            return Err("extrinsic payload is empty".into());
        }
        let wire = subtensor_ipc::encode_opaque_wire(&inner);
        let opaque = OpaqueExtrinsic::from_bytes(&mut &wire[..])
            .map_err(|e| format!("opaque extrinsic: {e}"))?;
        let at = best_hash();
        let tx_hash = pool
            .submit_local(at, opaque)
            .map_err(|e| format!("pool submit: {e:?}"))?;
        log::info!(
            target: "bot::ipc",
            "transaction submitted (prepared) hash={tx_hash:?}",
        );
        if let Some(tracker) = tx_inclusion_tracker {
            tracker.register_submitted(format!("{tx_hash:?}"));
        }
        if let Some(propagator) = tx_propagator {
            propagator.propagate(tx_hash);
        }
        return Ok(());
    }

    // Legacy path: full wire hex; hex-decode + SCALE-decode on the node.
    let bytes = subtensor_ipc::decode_hex(&hash)?;

    if let Ok(opaque) = OpaqueExtrinsic::decode(&mut &bytes[..]) {
        let at = best_hash();
        let tx_hash = pool
            .submit_local(at, opaque)
            .map_err(|e| format!("pool submit: {e:?}"))?;
        log::info!(
            target: "bot::ipc",
            "transaction submitted hash={tx_hash:?}",
        );
        if let Some(tracker) = tx_inclusion_tracker {
            tracker.register_submitted(format!("{tx_hash:?}"));
        }
        if let Some(propagator) = tx_propagator {
            propagator.propagate(tx_hash);
        }
        return Ok(());
    }

    if bytes.len() == 32 {
        return Err("submit opaque extrinsic hex to import a transaction".into());
    }

    Err("transaction: expected hex-encoded opaque extrinsic (or use extrinsic field)".into())
}
