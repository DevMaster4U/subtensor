//! Earliest public hook for block announces on a non-validator node.
//!
//! `BlockAnnounceValidator::validate` is invoked as soon as the sync engine
//! hands off a network block announcement. We notify the bot synchronously at
//! that point - before async announce validation runs - so the processor can
//! submit to the pool while validation and block download proceed in parallel.
//!
//! Deeper hooks (inside the network worker, before the validator queue) are
//! not exposed by Substrate's public API. Validator proposer injection is
//! faster still but requires an authority role.

use futures::FutureExt;
use node_subtensor_runtime::opaque::Block;
use sp_blockchain::HeaderBackend;
use sp_consensus::block_validation::{
    BlockAnnounceValidator, DefaultBlockAnnounceValidator, Validation,
};
use sp_runtime::traits::Block as BlockT;
use std::pin::Pin;
use std::sync::Arc;
use subtensor_bot::announce::{self, BlockAnnounceHub};

use crate::client::FullClient;

pub struct NotifyingBlockAnnounceValidator {
    inner: DefaultBlockAnnounceValidator,
    client: Arc<FullClient>,
    hub: BlockAnnounceHub,
}

impl NotifyingBlockAnnounceValidator {
    pub fn new(client: Arc<FullClient>, hub: BlockAnnounceHub) -> Self {
        Self {
            inner: DefaultBlockAnnounceValidator,
            client,
            hub,
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
        if announce::is_ahead_of_best(header, best_number) {
            self.hub.notify(header);
            log::debug!(
                target: "bot::announce",
                "pre-validation announce #{} (local best #{best_number}) hash={:?}",
                header.number(),
                header.hash(),
            );
        }

        let validation = self.inner.validate(header, data);

        async move { validation.await }.boxed()
    }
}
