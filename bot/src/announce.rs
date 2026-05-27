//! Pre-import block announce notifications forwarded from the network layer.
//!
//! Signal timeline on a non-validator node (fastest → slowest):
//!   1. `BlockAnnounceValidator::validate()` entry — sync pool inject + hub notify
//!   2. Announce validation completes
//!   3. Block body download + import
//!   4. `client.import_notification_stream()` (inclusion / nonce advance only)
//!
//! Hybrid mode combines pool-front pre-submit with sync announce refresh.
//! Validator proposer injection is faster but requires authority role.

use node_subtensor_runtime::opaque::Block;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};
use tokio::sync::broadcast;

/// A block header received from the network announce path.
#[derive(Clone, Debug)]
pub struct BlockAnnounceNotification {
    pub number: u32,
    pub hash: <Block as BlockT>::Hash,
    pub parent_hash: <Block as BlockT>::Hash,
}

/// Returns true when `header` is the immediate next block after local best.
///
/// Only this case triggers inject — not farther-ahead announces (`best + 2`, …),
/// which would otherwise burn the send budget before those blocks import.
pub fn is_immediate_next_block<H>(header: &H, best_number: u32) -> bool
where
    H: HeaderT<Number = u32>,
{
    *header.number() == best_number.saturating_add(1)
}

/// Deprecated alias — prefer [`is_immediate_next_block`].
pub fn is_ahead_of_best<H>(header: &H, best_number: u32) -> bool
where
    H: HeaderT<Number = u32>,
{
    is_immediate_next_block(header, best_number)
}

/// Broadcast hub for pre-import block announce events.
#[derive(Clone)]
pub struct BlockAnnounceHub {
    tx: broadcast::Sender<BlockAnnounceNotification>,
}

impl BlockAnnounceHub {
    pub fn new() -> (Self, broadcast::Receiver<BlockAnnounceNotification>) {
        let (tx, rx) = broadcast::channel(256);
        (Self { tx }, rx)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BlockAnnounceNotification> {
        self.tx.subscribe()
    }

    pub fn notify<H>(&self, header: &H)
    where
        H: HeaderT<Number = u32, Hash = <Block as BlockT>::Hash>,
    {
        let number = *header.number();
        let hash = header.hash();
        let parent_hash = *header.parent_hash();
        let _ = self.tx.send(BlockAnnounceNotification {
            number,
            hash,
            parent_hash,
        });
    }
}
