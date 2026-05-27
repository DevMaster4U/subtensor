//! Shared transaction-injection state used by pool-front, announce, and sync hooks.

use crate::transact::{PrebuiltTx, TxConfig, TxPropagator, fetch_nonce, prebuild};
use fp_rpc::EthereumRuntimeRPCApi;
use node_subtensor_runtime::opaque::Block;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool, error::IntoPoolError};
use sp_core::U256;
use sp_runtime::{
    transaction_validity::InvalidTransaction,
    traits::Block as BlockT,
};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct PendingTx {
    pub tx: PrebuiltTx,
    pub nonce: U256,
}

#[derive(Default)]
struct Inner {
    ready: bool,
    pending: Option<PendingTx>,
    queued_nonce: Option<U256>,
    inject_paused: bool,
    last_announce_inject_at: Option<u32>,
}

/// Injection state shared across async tasks and the sync announce hook.
pub struct SharedInjectState {
    inner: RwLock<Inner>,
}

impl SharedInjectState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner::default()),
        })
    }

    pub fn init_pending(&self, pending: PendingTx) {
        let mut inner = self.inner.write().expect("poisoned");
        inner.pending = Some(pending);
        inner.ready = true;
        inner.queued_nonce = None;
        inner.inject_paused = false;
        inner.last_announce_inject_at = None;
    }

    pub fn resync_pending(&self, pending: PendingTx) {
        let mut inner = self.inner.write().expect("poisoned");
        inner.pending = Some(pending);
        inner.ready = true;
        inner.queued_nonce = None;
        inner.inject_paused = false;
    }

    pub fn is_ready(&self) -> bool {
        self.inner.read().expect("poisoned").ready
    }

    pub fn pending(&self) -> Option<PendingTx> {
        let inner = self.inner.read().expect("poisoned");
        if inner.ready {
            inner.pending.clone()
        } else {
            None
        }
    }

    pub fn queued_nonce(&self) -> Option<U256> {
        self.inner.read().expect("poisoned").queued_nonce
    }

    pub fn inject_paused(&self) -> bool {
        self.inner.read().expect("poisoned").inject_paused
    }

    pub fn set_inject_paused(&self, paused: bool) {
        self.inner.write().expect("poisoned").inject_paused = paused;
    }

    pub fn clear_inject_paused(&self) {
        self.set_inject_paused(false);
    }

    pub fn mark_queued(&self, nonce: U256) {
        self.inner.write().expect("poisoned").queued_nonce = Some(nonce);
    }

    pub fn clear_queued(&self) {
        self.inner.write().expect("poisoned").queued_nonce = None;
    }

    pub fn advance_pending(&self, cfg: &TxConfig, chain_nonce: U256) -> PendingTx {
        let pending = build_tx_at(cfg, chain_nonce);
        let mut inner = self.inner.write().expect("poisoned");
        inner.pending = Some(pending.clone());
        inner.queued_nonce = None;
        pending
    }

    pub fn set_pending(&self, pending: PendingTx) {
        let mut inner = self.inner.write().expect("poisoned");
        inner.pending = Some(pending);
        inner.ready = true;
    }

    /// Returns `true` when this block has not yet received an announce-path inject.
    pub fn try_claim_announce_block(&self, block_number: u32) -> bool {
        let mut inner = self.inner.write().expect("poisoned");
        if inner.last_announce_inject_at == Some(block_number) {
            return false;
        }
        inner.last_announce_inject_at = Some(block_number);
        true
    }

    pub fn clear_announce_claim(&self, block_number: u32) {
        let mut inner = self.inner.write().expect("poisoned");
        if inner.last_announce_inject_at == Some(block_number) {
            inner.last_announce_inject_at = None;
        }
    }
}

pub fn build_tx_at(cfg: &TxConfig, nonce: U256) -> PendingTx {
    PendingTx {
        tx: prebuild(cfg, nonce, vec![0u8; 4]),
        nonce,
    }
}

pub enum InjectResult {
    Queued,
    Retry,
    Fatal,
}

pub fn classify_pool_error<E: IntoPoolError>(err: E) -> InjectResult {
    match err.into_pool_error() {
        Ok(sc_transaction_pool_api::error::Error::AlreadyImported(_)) => InjectResult::Queued,
        Ok(sc_transaction_pool_api::error::Error::TemporarilyBanned) => InjectResult::Fatal,
        Ok(sc_transaction_pool_api::error::Error::InvalidTransaction(
            InvalidTransaction::Payment,
        )) => InjectResult::Fatal,
        Ok(_) => InjectResult::Retry,
        Err(_) => InjectResult::Retry,
    }
}

/// Submit to the local pool synchronously (used from the announce validator thread).
pub fn inject_sync<P>(
    pool: &P,
    at_hash: <Block as BlockT>::Hash,
    pending: &PendingTx,
    propagator: &TxPropagator,
) -> InjectResult
where
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >,
{
    log::info!(
        target: "bot::sync_inject",
        "📌 sync inject (nonce={}, at={:?})",
        pending.nonce,
        at_hash,
    );

    match pool.submit_local(at_hash, pending.tx.extrinsic.clone()) {
        Ok(hash) => {
            log::info!(
                target: "bot::sync_inject",
                "✅ tx in ready pool, hash = {:?}",
                hash,
            );
            propagator.propagate(hash);
            InjectResult::Queued
        }
        Err(e) => classify_pool_error(e),
    }
}

pub fn resync_pending<C>(client: &C, cfg: &TxConfig) -> Result<PendingTx, String>
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    let nonce = fetch_nonce(client, cfg.from)?;
    Ok(build_tx_at(cfg, nonce))
}
