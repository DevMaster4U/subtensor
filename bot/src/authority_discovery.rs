//! RPC-facing facade for Aura authority queries and peer correlation.

use crate::authorities::{self, AuraAuthority, AuraSchedule};
use crate::authority_peers::{
    ApplyAuthorityReservedResult, AuthorityPeerMapping, AuthorityPeerRegistry,
    ConnectedAuthorityPeer, connected_authority_peers,
};
use crate::peers::{PeerPruner, PeerTracker};
use node_subtensor_runtime::opaque::Block;
use sc_network::NetworkStatusProvider;
use sc_network_sync::SyncingService;
use sp_api::ProvideRuntimeApi;
use sp_blockchain::HeaderBackend;
use sp_consensus_aura::{AuraApi, sr25519::AuthorityId as AuraId};
use std::sync::Arc;

/// Type-erased backend for authority RPC handlers.
pub trait AuthorityRpcBackend: Send + Sync {
    fn aura_authorities(&self) -> Result<Vec<AuraAuthority>, String>;
    fn aura_schedule(&self, upcoming: u32) -> Result<AuraSchedule, String>;
    fn authority_peer_mappings(&self) -> Vec<AuthorityPeerMapping>;
    fn connected_authority_peers(&self) -> Result<Vec<ConnectedAuthorityPeer>, String>;
    fn export_authority_reserved(&self, path: &str, min_hits: u64) -> Result<Vec<String>, String>;
    fn apply_authority_reserved(&self, min_hits: u64) -> Result<ApplyAuthorityReservedResult, String>;
}

pub struct AuthorityDiscovery<C> {
    client: Arc<C>,
    registry: Arc<AuthorityPeerRegistry>,
    sync: Arc<SyncingService<Block>>,
    peer_tracker: Arc<PeerTracker>,
    network: Arc<dyn NetworkStatusProvider + Send + Sync>,
    peer_pruner: Arc<PeerPruner>,
}

impl<C> AuthorityDiscovery<C>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
    C::Api: AuraApi<Block, AuraId>,
{
    pub fn new(
        client: Arc<C>,
        registry: Arc<AuthorityPeerRegistry>,
        sync: Arc<SyncingService<Block>>,
        peer_tracker: Arc<PeerTracker>,
        network: Arc<dyn NetworkStatusProvider + Send + Sync>,
        peer_pruner: Arc<PeerPruner>,
    ) -> Arc<dyn AuthorityRpcBackend>
    where
        C: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
        C::Api: AuraApi<Block, AuraId>,
    {
        Arc::new(AuthorityDiscovery {
            client,
            registry,
            sync,
            peer_tracker,
            network,
            peer_pruner,
        })
    }

    pub fn aura_authorities(&self) -> Result<Vec<AuraAuthority>, String> {
        let at = self.client.info().best_hash;
        authorities::fetch_aura_authorities(self.client.as_ref(), at)
    }

    pub fn aura_schedule(&self, upcoming: u32) -> Result<AuraSchedule, String> {
        authorities::fetch_aura_schedule(self.client.as_ref(), upcoming)
    }

    pub fn authority_peer_mappings(&self) -> Vec<AuthorityPeerMapping> {
        let addrs = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.network.connected_peer_addresses())
        });
        let mut rows = self.registry.mappings();
        for row in &mut rows {
            if row.multiaddr.is_none() {
                row.multiaddr = addrs.get(&row.peer_id).cloned();
            }
        }
        rows
    }

    pub async fn connected_authority_peers(&self) -> Result<Vec<ConnectedAuthorityPeer>, String> {
        let addrs = self.network.connected_peer_addresses().await;
        connected_authority_peers(
            self.sync.clone(),
            self.peer_tracker.clone(),
            self.registry.clone(),
            addrs,
        )
        .await
    }

    pub fn export_authority_reserved(&self, path: &str, min_hits: u64) -> Result<Vec<String>, String> {
        self.registry.export_reserved_file(path, min_hits)
    }

    pub async fn apply_authority_reserved(
        &self,
        min_hits: u64,
    ) -> Result<ApplyAuthorityReservedResult, String> {
        let addrs = self.registry.reserved_multiaddrs(min_hits);
        if addrs.is_empty() {
            return Ok(ApplyAuthorityReservedResult {
                added_count: 0,
                skipped_count: 0,
                peers: Vec::new(),
            });
        }

        let tmp = std::env::temp_dir().join(format!(
            "bot_authority_reserved_{}.txt",
            std::process::id()
        ));
        let path = tmp.to_string_lossy().to_string();
        std::fs::write(&path, addrs.join("\n")).map_err(|e| format!("write temp file: {e}"))?;

        let result = self.peer_pruner.set_reserved_from_file(&path, false).await?;
        let _ = std::fs::remove_file(&path);

        Ok(ApplyAuthorityReservedResult {
            added_count: result.added_count,
            skipped_count: addrs.len().saturating_sub(result.added_count as usize) as u32,
            peers: result.peers,
        })
    }
}

impl<C> AuthorityRpcBackend for AuthorityDiscovery<C>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
    C::Api: AuraApi<Block, AuraId>,
{
    fn aura_authorities(&self) -> Result<Vec<AuraAuthority>, String> {
        AuthorityDiscovery::aura_authorities(self)
    }

    fn aura_schedule(&self, upcoming: u32) -> Result<AuraSchedule, String> {
        AuthorityDiscovery::aura_schedule(self, upcoming)
    }

    fn authority_peer_mappings(&self) -> Vec<AuthorityPeerMapping> {
        AuthorityDiscovery::authority_peer_mappings(self)
    }

    fn connected_authority_peers(&self) -> Result<Vec<ConnectedAuthorityPeer>, String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(AuthorityDiscovery::connected_authority_peers(self))
        })
    }

    fn export_authority_reserved(&self, path: &str, min_hits: u64) -> Result<Vec<String>, String> {
        AuthorityDiscovery::export_authority_reserved(self, path, min_hits)
    }

    fn apply_authority_reserved(&self, min_hits: u64) -> Result<ApplyAuthorityReservedResult, String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(AuthorityDiscovery::apply_authority_reserved(self, min_hits))
        })
    }
}
