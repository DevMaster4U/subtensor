//! Aura slot ticks for full nodes: same `on_slot` timing as validators, without block authoring.

use node_subtensor_runtime::opaque::Block;
use sc_consensus_slots::{InherentDataProviderExt, SlotInfo, SlotWorker, start_slot_worker};
use sc_service::SpawnTaskHandle;
use sp_consensus::{SelectChain, SyncOracle};
use sp_consensus_slots::SlotDuration;
use sp_inherents::CreateInherentDataProviders;

struct SlotNotifyWorker;

#[async_trait::async_trait]
impl SlotWorker<Block, ()> for SlotNotifyWorker {
    async fn on_slot(
        &mut self,
        _slot_info: SlotInfo<Block>,
    ) -> Option<sc_consensus_slots::SlotResult<Block, ()>> {
        None
    }
}

/// Start the Aura slot watcher (full nodes only; validators use `start_authoring` instead).
pub fn start<CIDP, SC, SO>(
    spawn_handle: &SpawnTaskHandle,
    slot_duration: SlotDuration,
    select_chain: SC,
    sync_oracle: SO,
    create_inherent_data_providers: CIDP,
) where
    CIDP: CreateInherentDataProviders<Block, ()> + Send + Sync + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<Block> + Send + 'static,
    SO: SyncOracle + Send + Sync + 'static,
{
    spawn_handle.spawn_blocking(
        "bot-slot-watcher",
        None,
        start_slot_worker(
            slot_duration,
            select_chain,
            SlotNotifyWorker,
            sync_oracle,
            create_inherent_data_providers,
        ),
    );
}
