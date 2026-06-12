//! Shared prepared-extrinsic submit path (IPC fast path and `node_submitPreparedExtrinsic` RPC).

use std::sync::{Arc, RwLock};

use codec::Decode;
use node_subtensor_runtime::opaque::Block;
use sc_network::PeerId;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_core::H256;
use sp_runtime::OpaqueExtrinsic;

use crate::metrics_log::TxInclusionTracker;
use crate::transact::TxPropagator;
use crate::tx_propagation::{PropagateMode, RankFunction, TxPropagationControl, TxPropagationRequest};

/// Parameters for a prepared-extrinsic submit (mirrors IPC `Transaction` message).
#[derive(Clone, Debug, Default)]
pub struct PreparedSubmitRequest {
    /// Legacy full wire hex (`0x` + SCALE opaque extrinsic). Used when `extrinsic` is absent.
    pub hash: String,
    /// Fast path: hex-encoded inner opaque payload (client already validated SCALE).
    pub extrinsic: Option<String>,
    pub propagate_type: Option<String>,
    pub propagate_param: Option<String>,
    pub peer_id: Option<String>,
}

impl PreparedSubmitRequest {
    pub fn from_prepared_extrinsic(extrinsic_hex: impl Into<String>) -> Self {
        Self {
            extrinsic: Some(extrinsic_hex.into()),
            ..Default::default()
        }
    }
}

pub fn parse_propagation_request(
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

/// Submit a prepared extrinsic to the pool and optionally gossip it.
pub fn submit_prepared_extrinsic<P>(
    request: &PreparedSubmitRequest,
    pool: &Arc<P>,
    best_hash: &Arc<dyn Fn() -> H256 + Send + Sync>,
    tx_propagation: Option<&TxPropagationControl>,
    tx_propagator: Option<&TxPropagator>,
    tx_inclusion_tracker: Option<&TxInclusionTracker>,
    direct_peer: Option<PeerId>,
) -> Result<String, String>
where
    P: TransactionPool<Block = Block, Hash = H256>
        + LocalTransactionPool<Block = Block, Hash = H256>
        + 'static,
{
    if direct_peer.is_none() {
        if let Some(tx_control) = tx_propagation {
            if let Some(prop_request) =
                parse_propagation_request(request.propagate_type.clone(), request.propagate_param.clone())
            {
                tx_control.set_pending_request(prop_request);
            }
        }
    }

    let propagate = |propagator: &TxPropagator, tx_hash: H256| {
        if let Some(peer) = direct_peer {
            propagator.propagate_to_peer(tx_hash, peer);
        } else {
            propagator.propagate(tx_hash);
        }
    };

    if let Some(inner_hex) = request.extrinsic.as_ref() {
        let inner = subtensor_ipc::decode_hex(inner_hex)?;
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
            target: "bot::submit",
            "prepared extrinsic submitted hash={tx_hash:?} direct_peer={}",
            direct_peer
                .as_ref()
                .map(|p| p.to_base58())
                .unwrap_or_else(|| "none".into()),
        );
        if let Some(tracker) = tx_inclusion_tracker {
            tracker.register_submitted(format!("{tx_hash:?}"));
        }
        if let Some(propagator) = tx_propagator {
            propagate(propagator, tx_hash);
        }
        return Ok(format!("{tx_hash:?}"));
    }

    let bytes = subtensor_ipc::decode_hex(&request.hash)?;
    if let Ok(opaque) = OpaqueExtrinsic::decode(&mut &bytes[..]) {
        let at = best_hash();
        let tx_hash = pool
            .submit_local(at, opaque)
            .map_err(|e| format!("pool submit: {e:?}"))?;
        log::info!(
            target: "bot::submit",
            "wire extrinsic submitted hash={tx_hash:?}",
        );
        if let Some(tracker) = tx_inclusion_tracker {
            tracker.register_submitted(format!("{tx_hash:?}"));
        }
        if let Some(propagator) = tx_propagator {
            propagate(propagator, tx_hash);
        }
        return Ok(format!("{tx_hash:?}"));
    }

    if bytes.len() == 32 {
        return Err("submit opaque extrinsic hex to import a transaction".into());
    }

    Err("expected hex-encoded opaque extrinsic (or use extrinsic field)".into())
}

/// Type-erased handle wired from `service.rs` into IPC and node RPC.
pub trait PreparedExtrinsicSubmitter: Send + Sync {
    fn submit(&self, request: PreparedSubmitRequest) -> Result<String, String>;
}

struct PreparedExtrinsicSubmitterImpl<P> {
    pool: Arc<P>,
    best_hash: Arc<dyn Fn() -> H256 + Send + Sync>,
    tx_propagation: Arc<TxPropagationControl>,
    tx_propagator: Option<TxPropagator>,
    tx_inclusion_tracker: Arc<TxInclusionTracker>,
}

impl<P> PreparedExtrinsicSubmitter for PreparedExtrinsicSubmitterImpl<P>
where
    P: TransactionPool<Block = Block, Hash = H256>
        + LocalTransactionPool<Block = Block, Hash = H256>
        + 'static,
{
    fn submit(&self, request: PreparedSubmitRequest) -> Result<String, String> {
        let direct_peer = request
            .peer_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(crate::transact::parse_propagation_peer_id)
            .transpose()?;

        submit_prepared_extrinsic(
            &request,
            &self.pool,
            &self.best_hash,
            Some(&self.tx_propagation),
            self.tx_propagator.as_ref(),
            Some(&self.tx_inclusion_tracker),
            direct_peer,
        )
    }
}

/// Shared submit controls for IPC and `node_submitPreparedExtrinsic`.
pub struct TxSubmitHandle {
    inner: RwLock<Option<Arc<dyn PreparedExtrinsicSubmitter>>>,
}

impl TxSubmitHandle {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(None),
        })
    }

    pub fn set_controls<P>(
        self: &Arc<Self>,
        pool: Arc<P>,
        best_hash: Arc<dyn Fn() -> H256 + Send + Sync>,
        tx_propagation: Arc<TxPropagationControl>,
        tx_propagator: TxPropagator,
        tx_inclusion_tracker: Arc<TxInclusionTracker>,
    ) where
        P: TransactionPool<Block = Block, Hash = H256>
            + LocalTransactionPool<Block = Block, Hash = H256>
            + 'static,
    {
        let submitter: Arc<dyn PreparedExtrinsicSubmitter> =
            Arc::new(PreparedExtrinsicSubmitterImpl {
                pool,
                best_hash,
                tx_propagation,
                tx_propagator: Some(tx_propagator),
                tx_inclusion_tracker,
            });
        *self.inner.write().expect("poisoned") = Some(submitter);
    }

    pub fn submit(&self, request: PreparedSubmitRequest) -> Result<String, String> {
        self.inner
            .read()
            .expect("poisoned")
            .as_ref()
            .ok_or_else(|| "tx submit controls not initialized".to_string())?
            .submit(request)
    }
}
