//! Thread-local announcing peer passed into `BlockAnnounceValidator::validate()`.
//!
//! The sync engine knows which peer sent a block announce before calling the
//! external validator. This module exposes that peer id for the duration of the
//! synchronous `validate()` call.

use sc_network_types::PeerId;
use std::cell::RefCell;

thread_local! {
	static ANNOUNCE_PEER: RefCell<Option<PeerId>> = const { RefCell::new(None) };
}

/// Sets the announcing peer for the current thread until dropped.
pub struct AnnouncePeerGuard(PeerId);

impl AnnouncePeerGuard {
	pub fn new(peer_id: PeerId) -> Self {
		ANNOUNCE_PEER.with(|cell| {
			*cell.borrow_mut() = Some(peer_id);
		});
		Self(peer_id)
	}
}

impl Drop for AnnouncePeerGuard {
	fn drop(&mut self) {
		ANNOUNCE_PEER.with(|cell| {
			*cell.borrow_mut() = None;
		});
	}
}

/// Returns the peer that triggered the in-flight `validate()` call, if any.
pub fn current() -> Option<PeerId> {
	ANNOUNCE_PEER.with(|cell| *cell.borrow())
}
