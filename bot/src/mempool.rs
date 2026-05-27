//! Mempool watcher.
//!
//! `subscribe_mempool` subscribes to the transaction pool's low-level
//! import notification stream — fires every time a tx enters the ready queue.
//!
//! This uses `TransactionPool::import_notification_stream()` directly,
//! which is the same source that `author_submitAndWatchExtrinsic` RPC uses
//! internally. No RPC, no HTTP.

use codec::{Decode, Encode};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use sc_service::TaskManager;
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use sp_runtime::traits::ExtrinsicCall;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use node_subtensor_runtime::{
    opaque::Block,
    RuntimeCall, UncheckedExtrinsic,
};

/// Runtime control for the optional mempool watcher task.
pub struct MempoolWatcherControl {
    running: AtomicBool,
}

impl Default for MempoolWatcherControl {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(false),
        }
    }
}

impl MempoolWatcherControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        log::info!(target: "bot::mempool", "mempool watcher enabled");
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        log::info!(target: "bot::mempool", "mempool watcher disabled");
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Spawn a background task that logs every tx entering the ready pool when enabled.
///
/// Called from `service.rs` or alongside `start_bot`:
/// ```rust
/// subtensor_bot::mempool::start_mempool_watcher(
///     &task_manager,
///     transaction_pool.clone(),
///     mempool_watcher_control.clone(),
/// );
/// ```
pub fn start_mempool_watcher<P>(
    task_manager: &TaskManager,
    pool: Arc<P>,
    control: Arc<MempoolWatcherControl>,
) where
    P: TransactionPool<Block = Block> + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-mempool-watcher",
        None,
        watch(pool, control),
    );
}

fn watch<P>(pool: Arc<P>, control: Arc<MempoolWatcherControl>) -> BoxFuture<'static, ()>
where
    P: TransactionPool<Block = Block> + 'static,
{
    async move {
        loop {
            if !control.is_running() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            log::info!(
                target: "bot::mempool",
                "🔍 mempool watcher started"
            );

            let mut stream = pool.import_notification_stream();

            loop {
                tokio::select! {
                    tx_hash = stream.next() => {
                        let Some(tx_hash) = tx_hash else {
                            log::warn!(
                                target: "bot::mempool",
                                "mempool stream ended"
                            );
                            break;
                        };

                        if !control.is_running() {
                            break;
                        }

                        process_import(&pool, tx_hash).await;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if !control.is_running() {
                            break;
                        }
                    }
                }
            }

            log::info!(
                target: "bot::mempool",
                "mempool watcher paused"
            );
        }
    }
    .boxed()
}

async fn process_import<P>(pool: &Arc<P>, tx_hash: <P as TransactionPool>::Hash)
where
    P: TransactionPool<Block = Block> + 'static,
{
    for tx in pool.ready() {
        if tx.hash() != &tx_hash {
            continue;
        }

        let opaque = tx.data();

        // Pool stores opaque extrinsics; decode into the typed runtime extrinsic.
        let encoded = opaque.encode();
        match UncheckedExtrinsic::decode(&mut &encoded[..]) {
            Ok(ext) => {
                if let RuntimeCall::Ethereum(pallet_ethereum::Call::transact { transaction }) =
                    ext.call()
                {
                    log_ethereum_call(transaction);
                }
            }
            Err(err) => {
                log::warn!(
                    target: "bot::mempool",
                    "⚠️ failed to decode extrinsic: {:?}",
                    err
                );
            }
        }
    }

    let status = pool.status();
    log::debug!(
        target: "bot::mempool",
        "pool => ready={}, future={}",
        status.ready,
        status.future
    );
}

fn log_ethereum_call(transaction: &pallet_ethereum::Transaction) {
    let data: pallet_ethereum::TransactionData = transaction.into();
    let chain_id = data.chain_id.unwrap_or(0);
    let nonce = data.nonce.low_u64();

    log::info!(
        target: "bot::mempool",
        "⚡ Ethereum call: chain_id={}, nonce={}, action: {:?}",
        chain_id,
        nonce,
        data.action,
    );
}
