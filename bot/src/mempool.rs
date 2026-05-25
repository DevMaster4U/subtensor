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
use sp_runtime::{generic::Preamble, traits::ExtrinsicCall};
use std::sync::Arc;

use node_subtensor_runtime::{
    opaque::Block,
    RuntimeCall, UncheckedExtrinsic,
};

/// Spawn a background task that logs every tx entering the ready pool.
///
/// Called from `service.rs` or alongside `start_bot`:
/// ```rust
/// subtensor_bot::mempool::start_mempool_watcher(&task_manager, transaction_pool.clone());
/// ```
pub fn start_mempool_watcher<P>(task_manager: &TaskManager, pool: Arc<P>)
where
    P: TransactionPool<Block = Block> + 'static,
{
    task_manager.spawn_handle().spawn(
        "bot-mempool-watcher",
        None,
        watch(pool),
    );
}

fn watch<P>(pool: Arc<P>) -> BoxFuture<'static, ()>
where
    P: TransactionPool<Block = Block> + 'static,
{
    async move {
        log::info!(
            target: "bot::mempool",
            "🔍 mempool watcher started"
        );

        let mut stream = pool.import_notification_stream();

        while let Some(tx_hash) = stream.next().await {
            log::info!(
                target: "bot::mempool",
                "📥 tx hash: {:?}",
                tx_hash
            );

            for tx in pool.ready() {
                if tx.hash() != &tx_hash {
                    continue;
                }

                let opaque = tx.data();
                log::info!(
                    target: "bot::mempool",
                    "✅ extrinsic captured"
                );

                // Pool stores opaque extrinsics; decode into the typed runtime extrinsic.
                let encoded = opaque.encode();
                match UncheckedExtrinsic::decode(&mut &encoded[..]) {
                    Ok(ext) => {
                        let call = ext.call();
                        log::info!(
                            target: "bot::mempool",
                            "call = {:?}",
                            call
                        );
                        decode_call(call);

                        match &ext.0.preamble {
                            Preamble::Signed(address, signature, extra) => {
                                log::info!(
                                    target: "bot::mempool",
                                    "signer: {:?}",
                                    address
                                );
                                log::debug!(
                                    target: "bot::mempool",
                                    "signature: {:?}, extra: {:?}",
                                    signature,
                                    extra
                                );
                            }
                            Preamble::Bare(_) => {
                                log::debug!(
                                    target: "bot::mempool",
                                    "bare extrinsic (inherent or self-contained)"
                                );
                            }
                            Preamble::General(_, extra) => {
                                log::debug!(
                                    target: "bot::mempool",
                                    "general extrinsic, extra: {:?}",
                                    extra
                                );
                            }
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

        log::warn!(
            target: "bot::mempool",
            "mempool stream ended"
        );
    }
    .boxed()
}

/// Decode runtime calls
fn decode_call(call: &RuntimeCall) {
    match call {
        RuntimeCall::SubtensorModule(inner) => {
            log::info!(
                target: "bot::mempool",
                "📦 Subtensor call: {:?}",
                inner
            );
        }

        RuntimeCall::Balances(inner) => {
            log::info!(
                target: "bot::mempool",
                "💰 Balances call: {:?}",
                inner
            );
        }

        RuntimeCall::Sudo(inner) => {
            log::warn!(
                target: "bot::mempool",
                "⚠️ Sudo call: {:?}",
                inner
            );
        }

        RuntimeCall::Utility(inner) => {
            log::info!(
                target: "bot::mempool",
                "🛠 Utility call: {:?}",
                inner
            );
        }

        RuntimeCall::Ethereum(inner) => {
            log::info!(
                target: "bot::mempool",
                "⚡ Ethereum call: {:?}",
                inner
            );
        }

        _ => {
            log::info!(
                target: "bot::mempool",
                "📄 Other call: {:?}",
                call
            );
        }
    }
}
