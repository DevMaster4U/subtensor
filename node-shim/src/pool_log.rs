//! Always-on transaction pool import logging (independent of the mempool watcher).
//!
//! Subscribes to [`TransactionPool::import_notification_stream`] and logs each newly
//! imported transaction (hash + runtime call name).

use codec::{Decode, Encode};
use frame_support::traits::GetCallMetadata;
use futures::{future::BoxFuture, FutureExt, StreamExt};
use node_subtensor_runtime::UncheckedExtrinsic;
use sc_service::TaskManager;
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use node_subtensor_runtime::opaque::Block;

fn call_type_name(extrinsic: &<Block as sp_runtime::traits::Block>::Extrinsic) -> String {
    let bytes = extrinsic.encode();
    let Ok(xt) = UncheckedExtrinsic::decode(&mut &bytes[..]) else {
        return "decode_failed".into();
    };

    xt.0.function.get_call_metadata().function_name.to_string()
}

fn log_pool_import<P>(pool: &Arc<P>, tx_hash: <P as TransactionPool>::Hash)
where
    P: TransactionPool<Block = Block> + 'static,
{
    for tx in pool.ready() {
        if tx.hash() != &tx_hash {
            continue;
        }
        log::info!(
            target: "bot::pool",
            "pool import hash={tx_hash:?} {}",
            call_type_name(tx.data().as_ref()),
        );
        return;
    }

    log::info!(
        target: "bot::pool",
        "pool import hash={tx_hash:?} (not in ready queue — likely future/invalid)",
    );
}

/// Runtime toggle for pool import logging (`node_enableMempoolLog` / `node_enablePoolImportLog` RPC).
pub struct PoolImportLogControl {
    enabled: AtomicBool,
}

impl Default for PoolImportLogControl {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(true),
        }
    }
}

impl PoolImportLogControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
        log::info!(target: "bot::pool", "pool import log enabled");
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        log::info!(target: "bot::pool", "pool import log disabled");
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
}

/// Spawn background task logging each newly imported pool transaction.
pub fn start_pool_import_logger<P>(
    task_manager: &TaskManager,
    pool: Arc<P>,
    control: Arc<PoolImportLogControl>,
) where
    P: TransactionPool<Block = Block> + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-pool-import-log",
        None,
        watch(pool, control),
    );
}

fn watch<P>(pool: Arc<P>, control: Arc<PoolImportLogControl>) -> BoxFuture<'static, ()>
where
    P: TransactionPool<Block = Block> + 'static,
{
    async move {
        log::info!(target: "bot::pool", "pool import logger started");

        let mut stream = pool.import_notification_stream();

        loop {
            tokio::select! {
                tx_hash = stream.next() => {
                    let Some(tx_hash) = tx_hash else {
                        log::warn!(target: "bot::pool", "pool import stream ended, restarting…");
                        stream = pool.import_notification_stream();
                        continue;
                    };

                    if !control.is_enabled() {
                        continue;
                    }

                    log_pool_import(&pool, tx_hash);
                }
            }
        }
    }
    .boxed()
}

/// Log pool state immediately after a local submit (IPC path).
pub fn log_after_local_submit<P>(pool: &Arc<P>, tx_hash: <P as TransactionPool>::Hash)
where
    P: TransactionPool<Block = Block> + 'static,
{
    log::info!(target: "bot::pool", "local submit hash={tx_hash:?}");
    log_pool_import(pool, tx_hash);
}
