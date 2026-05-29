//! Thread-local announcing peer for [`sp_consensus::block_validation::BlockAnnounceValidator`]
//! hooks.
//!
//! The sync engine sets this immediately before calling `validate()` so node-side
//! announce handlers can attribute the exact libp2p peer.

use sc_network_types::PeerId;
use std::cell::RefCell;

thread_local! {
    static CURRENT: RefCell<Option<PeerId>> = const { RefCell::new(None) };
}

/// Record the peer whose block announce is entering validation.
pub fn set(peer_id: PeerId) {
    CURRENT.with(|slot| *slot.borrow_mut() = Some(peer_id));
}

/// Clear after the synchronous portion of `validate()` returns.
pub fn clear() {
    CURRENT.with(|slot| *slot.borrow_mut() = None);
}

/// Peer id for the in-flight block announce validation, if any.
pub fn current() -> Option<PeerId> {
    CURRENT.with(|slot| slot.borrow().clone())
}
