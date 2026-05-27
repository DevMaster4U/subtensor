// Patched: live peer-id → multiaddr map (PEER_MAP source of truth for litep2p).

use crate::network_state::{NetworkState, PeerEndpoint};
use parking_lot::RwLock;
use sc_network_types::PeerId;
use std::collections::HashMap;
use std::sync::Arc;

/// Latest connection multiaddr per peer, updated on `ConnectionEstablished`.
#[derive(Clone, Default)]
pub struct PeerConnectionMap(Arc<RwLock<HashMap<PeerId, String>>>);

impl std::fmt::Debug for PeerConnectionMap {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("PeerConnectionMap")
			.field("peers", &self.0.read().len())
			.finish()
	}
}

impl PeerConnectionMap {
	/// Create an empty map.
	pub fn new() -> Self {
		Self::default()
	}

	/// Record or refresh the address used for a live connection.
	pub fn record(&self, peer_id: PeerId, addr: impl Into<String>) {
		self.0.write().insert(peer_id, addr.into());
	}

	/// Drop a peer after all its connections close.
	pub fn remove(&self, peer_id: &PeerId) {
		self.0.write().remove(peer_id);
	}

	/// Snapshot as base58 peer id → multiaddr.
	pub fn snapshot(&self) -> HashMap<String, String> {
		self.0
			.read()
			.iter()
			.map(|(peer_id, addr)| (peer_id.to_base58(), addr.clone()))
			.collect()
	}
}

/// Extract peer addresses from a libp2p [`NetworkState`].
pub fn addrs_from_network_state(state: &NetworkState) -> HashMap<String, String> {
	let mut map = HashMap::new();

	for (peer_id, peer) in &state.connected_peers {
		if let Some(addr) = endpoint_addr(peer_id, &peer.endpoint) {
			map.insert(peer_id.clone(), addr);
		} else if let Some(addr) = peer.known_addresses.iter().next() {
			map.insert(peer_id.clone(), ensure_p2p_suffix(addr.to_string(), peer_id));
		}
	}

	for (peer_id, peer) in &state.not_connected_peers {
		map.entry(peer_id.clone()).or_insert_with(|| {
			peer.known_addresses
				.iter()
				.next()
				.map(|addr| ensure_p2p_suffix(addr.to_string(), peer_id))
				.unwrap_or_else(|| format!("/p2p/{peer_id}"))
		});
	}

	map
}

fn endpoint_addr(peer_id: &str, endpoint: &PeerEndpoint) -> Option<String> {
	let s = match endpoint {
		PeerEndpoint::Dialing(addr, _) => addr.to_string(),
		PeerEndpoint::Listening { send_back_addr, .. } => send_back_addr.to_string(),
	};
	Some(ensure_p2p_suffix(s, peer_id))
}

fn ensure_p2p_suffix(addr: String, peer_id: &str) -> String {
	if addr.contains("/p2p/") {
		addr
	} else {
		format!("{}/p2p/{}", addr.trim_end_matches('/'), peer_id)
	}
}
