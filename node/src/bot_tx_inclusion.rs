//! Chain-import watcher for bot transaction inclusion delay metrics.

use std::sync::Arc;

use futures::{FutureExt, StreamExt};
use sc_client_api::{BlockBackend, BlockchainEvents};
use sc_service::TaskManager;
use sp_core::{hashing::blake2_256, Encode, H256};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use subtensor_node_shim::{
    log_tx_inclusion_delay, MetricsLogControl, TxInclusionTracker,
};

use crate::client::FullClient;

pub fn start_tx_inclusion_watcher(
    task_manager: &TaskManager,
    client: Arc<FullClient>,
    control: Arc<MetricsLogControl>,
    tracker: Arc<TxInclusionTracker>,
) {
    task_manager.spawn_handle().spawn(
        "bot-tx-inclusion-metrics",
        None,
        async move {
            let mut stream = client.import_notification_stream();
            while let Some(notification) = stream.next().await {
                if !control.tx_inclusion_delay() {
                    continue;
                }
                let hash = notification.hash;
                let Ok(Some(signed)) = client.block(hash) else {
                    continue;
                };
                let block_number = *signed.block.header().number();
                let pending = tracker.pending_snapshot();
                if pending.is_empty() {
                    continue;
                }
                let mut found = Vec::new();
                for ext in &signed.block.extrinsics {
                    let opaque = ext.encode();
                    let ext_hash = format!("{:?}", H256::from(blake2_256(&opaque)));
                    if pending.contains_key(&ext_hash) {
                        found.push(ext_hash);
                    }
                }
                for tx_hash in found {
                    if let Some(submitted_ms) = tracker.take_pending(&tx_hash) {
                        log_tx_inclusion_delay(&control, &tx_hash, block_number, submitted_ms);
                    }
                }
            }
        }
        .boxed(),
    );
}
