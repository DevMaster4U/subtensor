//! Earliest public hook for block announces on a non-validator node.
//!
//! `BlockAnnounceValidator::validate` is invoked as soon as the sync engine
//! hands off a network block announcement. We notify the bot synchronously at
//! that point - before async announce validation runs - so the processor can
//! submit to the pool while validation and block download proceed in parallel.
//!
//! When a sync injector is installed, it also submits to the pool from this
//! hook (zero broadcast latency).
//!
//! Deeper hooks (inside the network worker, before the validator queue) are
//! not exposed by Substrate's public API. Validator proposer injection is
//! faster still but requires an authority role.

use futures::FutureExt;
use node_subtensor_runtime::opaque::Block;
use sc_network_sync::announce_peer;
use sp_blockchain::HeaderBackend;
use sp_consensus::block_validation::{
    BlockAnnounceValidator, DefaultBlockAnnounceValidator, Validation,
};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use std::pin::Pin;
use std::sync::Arc;
use subtensor_bot::announce::{self, BlockAnnounceHub};
use subtensor_bot::SyncInjectHandle;

use crate::client::FullClient;

pub struct NotifyingBlockAnnounceValidator {
    inner: DefaultBlockAnnounceValidator,
    client: Arc<FullClient>,
    hub: BlockAnnounceHub,
    sync_inject: Arc<SyncInjectHandle>,
}

impl NotifyingBlockAnnounceValidator {
    pub fn new(
        client: Arc<FullClient>,
        hub: BlockAnnounceHub,
        sync_inject: Arc<SyncInjectHandle>,
    ) -> Self {
        Self {
            inner: DefaultBlockAnnounceValidator,
            client,
            hub,
            sync_inject,
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
        if announce::is_immediate_next_block(header, best_number) {
            let block_number = *header.number();
            let at_hash = *header.parent_hash();
            let announcing_peer = announce_peer::current().map(|p| p.to_base58());
            self.sync_inject.on_announce(block_number, at_hash);
            self.hub
                .notify(header, announcing_peer.as_deref());
            log::debug!(
                target: "bot::announce",
                "pre-validation announce #{block_number} (local best #{best_number}) hash={:?} from={announcing_peer:?}",
                header.hash(),
            );
        }

        let validation = BlockAnnounceValidator::<Block>::validate(&mut self.inner, header, data);

        async move { validation.await }.boxed()
    }
}
