//! Learn `{ Aura account → libp2p PeerId / multiaddr }` from block production correlation.
//!
//! Subtensor removed authority discovery, so we attribute the first announce peer
//! for block `N` to the Aura author decoded from block `N`'s header digest.

use crate::authorities::{AuraAuthority, author_for_header, author_for_slot_and_parent};
use node_subtensor_runtime::opaque::Block;
use sp_api::ProvideRuntimeApi;
use sp_blockchain::HeaderBackend;
use sp_consensus_aura::{AuraApi, sr25519::AuthorityId as AuraId};
use sp_runtime::traits::Block as BlockT;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Learned network endpoint for one Aura authority account.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthorityPeerMapping {
    pub account: String,
    pub aura_index: u32,
    pub peer_id: String,
    pub multiaddr: Option<String>,
    /// Times this peer was first to announce a block authored by `account`.
    pub hits: u64,
    pub last_block: u32,
    pub roles: String,
}

/// Connected peer that advertises `AUTHORITY` role (may or may not be mapped yet).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConnectedAuthorityPeer {
    pub peer_id: String,
    pub multiaddr: Option<String>,
    pub roles: String,
    pub best_number: u64,
    pub mapped_account: Option<String>,
    pub announce_score: u64,
    pub tx_propagation_hits: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ApplyAuthorityReservedResult {
    pub added_count: u32,
    pub skipped_count: u32,
    pub peers: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedEntry {
    account: String,
    aura_index: u32,
    peer_id: String,
    hits: u64,
    last_block: u32,
    roles: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct PersistedFile {
    mappings: Vec<PersistedEntry>,
}

#[derive(Default)]
struct Inner {
    /// Best mapping per Aura account (highest hits).
    by_account: HashMap<String, AuthorityPeerMapping>,
    /// Reverse index for quick peer lookup.
    account_by_peer: HashMap<String, String>,
}

pub struct AuthorityPeerRegistry {
    inner: RwLock<Inner>,
    path: PathBuf,
}

impl AuthorityPeerRegistry {
    pub fn new() -> Arc<Self> {
        let registry = Arc::new(Self {
            inner: RwLock::new(Inner::default()),
            path: default_persist_path(),
        });
        registry.load_from_disk();
        registry
    }

    pub fn with_path(path: PathBuf) -> Arc<Self> {
        let registry = Arc::new(Self {
            inner: RwLock::new(Inner::default()),
            path,
        });
        registry.load_from_disk();
        registry
    }

    /// Correlate block author with the first announce peer for this height.
    pub fn record_block_author(
        &self,
        author: &AuraAuthority,
        block_number: u32,
        peer_id: &str,
        roles: &str,
        multiaddr: Option<String>,
    ) {
        let mut inner = self.inner.write().expect("poisoned");

        match inner.by_account.get_mut(&author.account) {
            Some(entry) if entry.peer_id == peer_id => {
                entry.hits = entry.hits.saturating_add(1);
                entry.aura_index = author.index;
                entry.last_block = block_number;
                if roles.contains("AUTHORITY") {
                    entry.roles = roles.to_string();
                }
                if multiaddr.is_some() {
                    entry.multiaddr = multiaddr;
                }
            }
            Some(entry) => {
                log::debug!(
                    target: "bot::authority_peers",
                    "block #{block_number}: author {} attributed to {peer_id}, mapped peer {} (hits={})",
                    author.account,
                    entry.peer_id,
                    entry.hits,
                );
                drop(inner);
                return;
            }
            None => {
                inner.by_account.insert(
                    author.account.clone(),
                    AuthorityPeerMapping {
                        account: author.account.clone(),
                        aura_index: author.index,
                        peer_id: peer_id.to_string(),
                        multiaddr,
                        hits: 1,
                        last_block: block_number,
                        roles: roles.to_string(),
                    },
                );
                inner
                    .account_by_peer
                    .insert(peer_id.to_string(), author.account.clone());
            }
        }

        let hits = inner
            .by_account
            .get(&author.account)
            .map(|e| e.hits)
            .unwrap_or(0);
        log::info!(
            target: "bot::authority_peers",
            "learned author {} → peer {peer_id} (hits={hits}, block #{block_number})",
            author.account,
        );

        drop(inner);
        let _ = self.persist();
    }

    pub fn mappings(&self) -> Vec<AuthorityPeerMapping> {
        let inner = self.inner.read().expect("poisoned");
        let mut rows: Vec<_> = inner.by_account.values().cloned().collect();
        rows.sort_by(|a, b| {
            b.hits
                .cmp(&a.hits)
                .then_with(|| a.account.cmp(&b.account))
        });
        rows
    }

    pub fn reserved_multiaddrs(&self, min_hits: u64) -> Vec<String> {
        self.mappings()
            .into_iter()
            .filter(|m| m.hits >= min_hits)
            .filter_map(|m| {
                m.multiaddr
                    .or_else(|| Some(format!("/p2p/{}", m.peer_id)))
            })
            .collect()
    }

    pub fn export_reserved_file(&self, path: &str, min_hits: u64) -> Result<Vec<String>, String> {
        let addrs = self.reserved_multiaddrs(min_hits);
        let body = addrs
            .iter()
            .map(|a| format!("{a}\n"))
            .collect::<String>();
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create dir {parent:?}: {e}"))?;
        }
        std::fs::write(path, body).map_err(|e| format!("write {path}: {e}"))?;
        log::info!(
            target: "bot::authority_peers",
            "exported {} authority reserved peer(s) to {path}",
            addrs.len(),
        );
        Ok(addrs)
    }

    fn load_from_disk(&self) {
        let Ok(body) = std::fs::read_to_string(&self.path) else {
            return;
        };
        let Ok(file) = serde_json::from_str::<PersistedFile>(&body) else {
            log::warn!(
                target: "bot::authority_peers",
                "failed to parse {:?}, starting fresh",
                self.path,
            );
            return;
        };
        let mut inner = self.inner.write().expect("poisoned");
        for entry in file.mappings {
            inner.by_account.insert(
                entry.account.clone(),
                AuthorityPeerMapping {
                    account: entry.account.clone(),
                    aura_index: entry.aura_index,
                    peer_id: entry.peer_id.clone(),
                    multiaddr: None,
                    hits: entry.hits,
                    last_block: entry.last_block,
                    roles: entry.roles,
                },
            );
            inner
                .account_by_peer
                .insert(entry.peer_id, entry.account);
        }
        log::info!(
            target: "bot::authority_peers",
            "loaded {} authority peer mapping(s) from {:?}",
            inner.by_account.len(),
            self.path,
        );
    }

    fn persist(&self) -> Result<(), String> {
        let inner = self.inner.read().expect("poisoned");
        let file = PersistedFile {
            mappings: inner
                .by_account
                .values()
                .map(|m| PersistedEntry {
                    account: m.account.clone(),
                    aura_index: m.aura_index,
                    peer_id: m.peer_id.clone(),
                    hits: m.hits,
                    last_block: m.last_block,
                    roles: m.roles.clone(),
                })
                .collect(),
        };
        drop(inner);

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create dir {parent:?}: {e}"))?;
        }
        let body = serde_json::to_string_pretty(&file)
            .map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(&self.path, body).map_err(|e| format!("write {:?}: {e}", self.path))?;
        Ok(())
    }
}

fn default_persist_path() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../authority_peers.json")
}

/// Resolve block author and correlate with the first announce peer.
pub async fn correlate_block_author<C>(
    client: Arc<C>,
    registry: Arc<AuthorityPeerRegistry>,
    block_number: u32,
    parent_hash: <Block as BlockT>::Hash,
    slot: Option<u64>,
    block_hash: <Block as BlockT>::Hash,
    first_peer_id: String,
    roles: String,
    multiaddr: Option<String>,
) where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
    C::Api: AuraApi<Block, AuraId>,
{
    // Block announces fire pre-import; the header is not in the DB yet. Use slot +
    // parent_hash from the announce notification when available.
    let author = if let Some(slot) = slot {
        match author_for_slot_and_parent(client.as_ref(), parent_hash, slot) {
            Ok(Some(a)) => a,
            Ok(None) => {
                log::debug!(
                    target: "bot::authority_peers",
                    "block #{block_number}: no Aura author for slot {slot}",
                );
                return;
            }
            Err(e) => {
                log::warn!(
                    target: "bot::authority_peers",
                    "block #{block_number}: author lookup failed: {e}",
                );
                return;
            }
        }
    } else {
        let header = match client.header(block_hash) {
            Ok(Some(h)) => h,
            Ok(None) => {
                log::debug!(
                    target: "bot::authority_peers",
                    "block #{block_number}: header not yet available for correlation",
                );
                return;
            }
            Err(e) => {
                log::debug!(
                    target: "bot::authority_peers",
                    "block #{block_number}: header fetch failed: {e}",
                );
                return;
            }
        };

        match author_for_header(client.as_ref(), &header) {
            Ok(Some(a)) => a,
            Ok(None) => {
                log::debug!(
                    target: "bot::authority_peers",
                    "block #{block_number}: no Aura author in digest",
                );
                return;
            }
            Err(e) => {
                log::warn!(
                    target: "bot::authority_peers",
                    "block #{block_number}: author lookup failed: {e}",
                );
                return;
            }
        }
    };

    registry.record_block_author(
        &author,
        block_number,
        &first_peer_id,
        &roles,
        multiaddr,
    );
}

/// List connected peers that advertise AUTHORITY role, enriched with tracker + mapping.
pub async fn connected_authority_peers(
    sync: Arc<sc_network_sync::SyncingService<Block>>,
    peer_tracker: Arc<crate::peers::PeerTracker>,
    registry: Arc<AuthorityPeerRegistry>,
    addrs: HashMap<String, String>,
) -> Result<Vec<ConnectedAuthorityPeer>, String> {
    use sp_runtime::traits::SaturatedConversion;

    let connected = sync
        .peers_info()
        .await
        .map_err(|_| "sync engine unavailable".to_string())?;

    let mut rows = Vec::new();
    for (peer_id, info) in connected {
        let roles = format!("{:?}", info.roles);
        if !roles.contains("AUTHORITY") {
            continue;
        }
        let id = peer_id.to_base58();
        let (announce_score, _, tx_hits, _, _) = peer_tracker
            .lookup(&id)
            .unwrap_or((0, 0, 0, 0, String::new()));
        let mapped_account = registry
            .mappings()
            .into_iter()
            .find(|m| m.peer_id == id)
            .map(|m| m.account);
        rows.push(ConnectedAuthorityPeer {
            peer_id: id.clone(),
            multiaddr: addrs.get(&id).cloned(),
            roles,
            best_number: info.best_number.saturated_into(),
            mapped_account,
            announce_score,
            tx_propagation_hits: tx_hits,
        });
    }

    rows.sort_by(|a, b| {
        b.announce_score
            .cmp(&a.announce_score)
            .then_with(|| b.best_number.cmp(&a.best_number))
    });

    Ok(rows)
}
