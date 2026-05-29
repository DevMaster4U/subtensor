//! Runtime reserved-peer set shared by peer pruning, tx ranking, and RPC.

use sc_network::PeerId;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

/// Reserved peer ids and whether the node is in reserved-only mode.
#[derive(Default)]
pub struct ReservedPeerRegistry {
    reserved_only: AtomicBool,
    peer_ids: RwLock<HashSet<PeerId>>,
}

impl ReservedPeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_reserved_only(&self) -> bool {
        self.reserved_only.load(Ordering::SeqCst)
    }

    pub fn set_reserved_only(&self, enabled: bool) {
        self.reserved_only.store(enabled, Ordering::SeqCst);
    }

    pub fn peer_ids(&self) -> HashSet<PeerId> {
        self.peer_ids.read().expect("reserved peer lock poisoned").clone()
    }

    pub fn replace_peers(&self, peers: impl IntoIterator<Item = PeerId>) {
        let mut guard = self.peer_ids.write().expect("reserved peer lock poisoned");
        guard.clear();
        guard.extend(peers);
    }

    pub fn add_peer(&self, peer: PeerId) {
        self.peer_ids
            .write()
            .expect("reserved peer lock poisoned")
            .insert(peer);
    }

    pub fn remove_peer(&self, peer: PeerId) {
        self.peer_ids
            .write()
            .expect("reserved peer lock poisoned")
            .remove(&peer);
    }

    pub fn contains(&self, peer: &PeerId) -> bool {
        self.peer_ids
            .read()
            .expect("reserved peer lock poisoned")
            .contains(peer)
    }
}
