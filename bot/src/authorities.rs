//! On-chain Aura authority queries and next-block-author prediction.
//!
//! Subtensor has no `pallet-authority-discovery`; consensus keys come from
//! [`AuraApi`] and network addresses are learned separately (see `authority_peers`).

use codec::Decode;
use node_subtensor_runtime::opaque::Block;
use sp_api::ProvideRuntimeApi;
use sp_blockchain::HeaderBackend;
use sp_consensus_aura::{AuraApi, AURA_ENGINE_ID};
use sp_consensus_aura::sr25519::AuthorityId as AuraId;
use sp_core::crypto::ByteArray;
use sp_runtime::generic::DigestItem;
use sp_runtime::traits::{Block as BlockT, Header};

/// One Aura authority from on-chain state.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuraAuthority {
    /// Authority index in the current set (0..n-1).
    pub index: u32,
    /// sr25519 public key / Subtensor block-author account (hex `0x…`).
    pub account: String,
}

/// Predicted author for a slot.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PredictedAuthor {
    pub slot: u64,
    pub index: u32,
    pub account: String,
}

/// Current Aura schedule snapshot.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuraSchedule {
    pub current_slot: u64,
    pub slot_duration_ms: u64,
    pub authority_count: u32,
    pub authorities: Vec<AuraAuthority>,
    pub next_authors: Vec<PredictedAuthor>,
}

pub fn aura_account_hex(id: &AuraId) -> String {
    format!("0x{}", hex::encode(id.to_raw_vec()))
}

pub fn slot_from_digest(digest: &sp_runtime::Digest) -> Option<u64> {
    for log in digest.logs() {
        if let DigestItem::PreRuntime(engine, data) = log {
            if engine == &AURA_ENGINE_ID {
                return u64::decode(&mut &data[..]).ok();
            }
        }
    }
    None
}

pub fn author_at_slot(authorities: &[AuraId], slot: u64) -> Option<(u32, AuraId)> {
    if authorities.is_empty() {
        return None;
    }
    let index = (slot as usize) % authorities.len();
    Some((index as u32, authorities[index].clone()))
}

pub fn predict_authors(authorities: &[AuraId], from_slot: u64, count: u32) -> Vec<PredictedAuthor> {
    let n = count.clamp(1, 32);
    (0..n)
        .filter_map(|offset| {
            let slot = from_slot.saturating_add(u64::from(offset));
            let (index, id) = author_at_slot(authorities, slot)?;
            Some(PredictedAuthor {
                slot,
                index,
                account: aura_account_hex(&id),
            })
        })
        .collect()
}

pub fn fetch_aura_authorities<C>(client: &C, at: <Block as BlockT>::Hash) -> Result<Vec<AuraAuthority>, String>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: AuraApi<Block, AuraId>,
{
    let authorities = client
        .runtime_api()
        .authorities(at)
        .map_err(|e| format!("AuraApi::authorities failed: {e}"))?;

    Ok(authorities
        .into_iter()
        .enumerate()
        .map(|(index, id)| AuraAuthority {
            index: index as u32,
            account: aura_account_hex(&id),
        })
        .collect())
}

pub fn fetch_slot_duration_ms<C>(client: &C, at: <Block as BlockT>::Hash) -> Result<u64, String>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: AuraApi<Block, AuraId>,
{
    let duration = client
        .runtime_api()
        .slot_duration(at)
        .map_err(|e| format!("AuraApi::slot_duration failed: {e}"))?;
    Ok(duration.as_millis())
}

pub fn author_for_slot_and_parent<C>(
    client: &C,
    parent_hash: <Block as BlockT>::Hash,
    slot: u64,
) -> Result<Option<AuraAuthority>, String>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: AuraApi<Block, AuraId>,
{
    let authorities = client
        .runtime_api()
        .authorities(parent_hash)
        .map_err(|e| format!("AuraApi::authorities failed: {e}"))?;
    let (index, id) = match author_at_slot(&authorities, slot) {
        Some(v) => v,
        None => return Ok(None),
    };
    Ok(Some(AuraAuthority {
        index,
        account: aura_account_hex(&id),
    }))
}

pub fn author_for_header<C>(
    client: &C,
    header: &<Block as BlockT>::Header,
) -> Result<Option<AuraAuthority>, String>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: AuraApi<Block, AuraId>,
{
    let slot = match slot_from_digest(header.digest()) {
        Some(s) => s,
        None => return Ok(None),
    };
    author_for_slot_and_parent(client, *header.parent_hash(), slot)
}

pub fn fetch_aura_schedule<C>(client: &C, upcoming: u32) -> Result<AuraSchedule, String>
where
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    C::Api: AuraApi<Block, AuraId>,
{
    let info = client.info();
    let at = info.best_hash;
    let header = client
        .header(at)
        .map_err(|e| format!("header lookup failed: {e}"))?
        .ok_or_else(|| "best header missing".to_string())?;

    let slot = slot_from_digest(header.digest()).unwrap_or(0);
    let slot_duration_ms = fetch_slot_duration_ms(client, at)?;
    let authorities_raw = client
        .runtime_api()
        .authorities(at)
        .map_err(|e| format!("AuraApi::authorities failed: {e}"))?;
    let authorities = authorities_raw
        .iter()
        .enumerate()
        .map(|(index, id)| AuraAuthority {
            index: index as u32,
            account: aura_account_hex(id),
        })
        .collect::<Vec<_>>();

    let next_authors = predict_authors(&authorities_raw, slot.saturating_add(1), upcoming);

    Ok(AuraSchedule {
        current_slot: slot,
        slot_duration_ms,
        authority_count: authorities.len() as u32,
        authorities,
        next_authors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codec::Encode;
    use sp_runtime::DigestItem;

    #[test]
    fn slot_from_aura_digest() {
        let slot = 42u64;
        let mut data = slot.encode();
        let digest = sp_runtime::Digest {
            logs: vec![DigestItem::PreRuntime(AURA_ENGINE_ID, data.clone())],
        };
        assert_eq!(slot_from_digest(&digest), Some(42));
    }
}
