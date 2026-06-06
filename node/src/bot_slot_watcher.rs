//! Aura slot ticks for full nodes: same `on_slot` timing as validators, without block authoring.

use std::sync::Arc;

use node_subtensor_runtime::opaque::Block;
use sc_consensus_aura::AuraApi;
use sc_consensus_slots::{InherentDataProviderExt, SlotInfo, SlotWorker, start_slot_worker};
use sc_service::SpawnTaskHandle;
use sp_api::ProvideRuntimeApi;
use sp_consensus::{SelectChain, SyncOracle};
use sp_consensus_aura::sr25519::AuthorityId as AuraAuthorityId;
use sp_consensus_slots::{Slot, SlotDuration};
use sp_inherents::CreateInherentDataProviders;

use crate::client::FullClient;

struct SlotNotifyWorker {
    client: Arc<FullClient>,
}

fn aura_authority_for_slot(authorities: &[AuraAuthorityId], slot: Slot) -> Option<AuraAuthorityId> {
    if authorities.is_empty() {
        return None;
    }
    let index = (*slot % authorities.len() as u64) as usize;
    authorities.get(index).cloned()
}

#[async_trait::async_trait]
impl SlotWorker<Block, ()> for SlotNotifyWorker {
    async fn on_slot(
        &mut self,
        slot_info: SlotInfo<Block>,
    ) -> Option<sc_consensus_slots::SlotResult<Block, ()>> {
        let slot = slot_info.slot;
        let hash = slot_info.chain_head.hash();
        let authorities = match self.client.runtime_api().authorities(hash) {
            Ok(authorities) if !authorities.is_empty() => authorities,
            Ok(_) => {
                log::warn!(target: "bot::slot", "on_slot slot={slot}: empty authority set");
                return None;
            }
            Err(err) => {
                log::warn!(
                    target: "bot::slot",
                    "on_slot slot={slot}: failed to fetch authorities at {hash:?}: {err}",
                );
                return None;
            }
        };

        let current_author = aura_authority_for_slot(&authorities, slot);
        let next_author = aura_authority_for_slot(&authorities, slot.saturating_add(1u64));
        log::info!(
            target: "bot::slot",
            "on_slot slot={slot} current_author={current_author:?} next_author={next_author:?}",
        );
        None
    }
}

/// Start the Aura slot watcher (full nodes only; validators use `start_authoring` instead).
pub fn start<CIDP, SC, SO>(
    spawn_handle: &SpawnTaskHandle,
    slot_duration: SlotDuration,
    client: Arc<FullClient>,
    select_chain: SC,
    sync_oracle: SO,
    create_inherent_data_providers: CIDP,
) where
    CIDP: CreateInherentDataProviders<Block, ()> + Send + Sync + 'static,
    CIDP::InherentDataProviders: InherentDataProviderExt + Send,
    SC: SelectChain<Block> + Send + 'static,
    SO: SyncOracle + Send + Sync + 'static,
{
    let worker = SlotNotifyWorker { client };
    spawn_handle.spawn_blocking(
        "bot-slot-watcher",
        None,
        start_slot_worker(
            slot_duration,
            select_chain,
            worker,
            sync_oracle,
            create_inherent_data_providers,
        ),
    );
    log::info!(target: "bot::slot", "slot watcher started (full node)");
}
