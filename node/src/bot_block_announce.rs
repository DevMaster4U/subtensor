//! Block announce hook: IPC events + peer/propagation tracking for the node shim.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::FutureExt;
use node_subtensor_runtime::opaque::Block;
use sc_network_sync::announce_peer;
use sp_blockchain::HeaderBackend;
use sp_consensus::block_validation::{
    BlockAnnounceValidator, DefaultBlockAnnounceValidator, Validation,
};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use subtensor_node_shim::announce::{self, current_delay_time_ms, slot_from_digest};
use subtensor_node_shim::metrics_log::{log_peer_announce_timing, MetricsLogControl};
use subtensor_node_shim::peer_scoreboard::PeerScoreboard;
use subtensor_node_shim::peers::PeerTracker;
use subtensor_node_shim::slot_state::SlotStateStore;
use subtensor_node_shim::{IpcManager, PropagationTracker};
use subtensor_ipc::IpcMessage;

use crate::client::FullClient;

pub struct NotifyingBlockAnnounceValidator {
    inner: DefaultBlockAnnounceValidator,
    client: Arc<FullClient>,
    ipc: Option<Arc<IpcManager>>,
    propagation_tracker: Arc<PropagationTracker>,
    peer_tracker: Arc<PeerTracker>,
    peer_scoreboard: Arc<PeerScoreboard>,
    slot_state: Arc<SlotStateStore>,
    metrics_log: Arc<MetricsLogControl>,
    /// Per-block announce count for IPC delivery.
    announce_counts: HashMap<u32, u32>,
}

impl NotifyingBlockAnnounceValidator {
    pub fn new(
        client: Arc<FullClient>,
        ipc: Option<Arc<IpcManager>>,
        propagation_tracker: Arc<PropagationTracker>,
        peer_tracker: Arc<PeerTracker>,
        peer_scoreboard: Arc<PeerScoreboard>,
        slot_state: Arc<SlotStateStore>,
        metrics_log: Arc<MetricsLogControl>,
    ) -> Self {
        Self {
            inner: DefaultBlockAnnounceValidator,
            client,
            ipc,
            propagation_tracker,
            peer_tracker,
            peer_scoreboard,
            slot_state,
            metrics_log,
            announce_counts: HashMap::new(),
        }
    }
}

impl BlockAnnounceValidator<Block> for NotifyingBlockAnnounceValidator {
    fn validate(
        &mut self,
        header: &<Block as BlockT>::Header,
        data: &[u8],
    ) -> Pin<
        Box<
            dyn futures::Future<Output = Result<Validation, Box<dyn std::error::Error + Send>>>
                + Send,
        >,
    > {
        let best_number = self.client.info().best_number;
        self.announce_counts.retain(|&n, _| n > best_number);

        if announce::is_immediate_next_block(header, best_number) {
            let block_number = *header.number();
            let delay_time_ms = current_delay_time_ms();
            let announcing_peer = announce_peer::current().map(|p| p.to_base58());
            let announce_index = {
                let count = self
                    .announce_counts
                    .entry(block_number)
                    .and_modify(|c| *c = c.saturating_add(1))
                    .or_insert(1);
                *count
            };

            if announce_index == 1 {
                log::info!(
                    target: "bot::announce",
                    "first announce detected: block #{block_number} hash={:?} parent={:?} (local best #{best_number})",
                    header.hash(),
                    header.parent_hash(),
                );

                self.propagation_tracker
                    .record_announce(block_number, announcing_peer.clone());
                self.peer_tracker.record_announce(
                    block_number,
                    std::iter::empty::<(String, u64, String)>(),
                    announcing_peer.as_deref(),
                );
            } else {
                log::trace!(
                    target: "bot::announce",
                    "announce #{block_number} index={announce_index} hash={:?} from={announcing_peer:?} (local best #{best_number})",
                    header.hash(),
                );
            }

            if let Some(ref peer) = announcing_peer {
                self.peer_tracker
                    .record_announce_peer(block_number, peer, delay_time_ms);
                self.peer_scoreboard.record_block_announce(
                    block_number,
                    peer,
                    delay_time_ms,
                    announce_index == 1,
                );
                self.slot_state.record_announce(
                    block_number,
                    peer,
                    delay_time_ms,
                    announce_index == 1,
                );
                log_peer_announce_timing(
                    &self.metrics_log,
                    block_number,
                    peer,
                    announce_index,
                    delay_time_ms,
                );
            }

            if let Some(ipc) = &self.ipc {
                ipc.notify_header(IpcMessage::header(
                    block_number,
                    format!("{:?}", header.hash()),
                    format!("{:?}", header.parent_hash()),
                    slot_from_digest(header.digest()),
                    announcing_peer,
                    announce_index,
                    delay_time_ms,
                ));
            }
        }

        let validation =
            BlockAnnounceValidator::<Block>::validate(&mut self.inner, header, data);
        async move { validation.await }.boxed()
    }
}
