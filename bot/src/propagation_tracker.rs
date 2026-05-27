//! Tracks bot-initiated transaction propagation correlated with block announces.

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const HISTORY_CAP: usize = 100;

/// Latest block announce context used to annotate the next bot propagation record.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AnnounceContext {
    pub block_number: u32,
    pub announcing_peer_id: Option<String>,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PropagationPeerInfo {
    pub peer_id: String,
    pub addr: Option<String>,
}

/// One completed bot-initiated propagation round.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OwnPropagationRecord {
    pub tx_hash: String,
    pub last_block_number: Option<u32>,
    pub announced_peer_id: Option<String>,
    pub timestamp_ms: u64,
    pub propagate_time_ms: u64,
    pub propagate_peers: Vec<PropagationPeerInfo>,
}

#[derive(Default)]
struct Inner {
    announce: Option<AnnounceContext>,
    pending_tx_hash: Option<String>,
    last: Option<OwnPropagationRecord>,
    history: VecDeque<OwnPropagationRecord>,
}

pub struct PropagationTracker {
    inner: RwLock<Inner>,
}

impl PropagationTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner::default()),
        })
    }

    pub fn record_announce(&self, block_number: u32, announcing_peer_id: Option<String>) {
        let mut inner = self.inner.write().expect("poisoned");
        inner.announce = Some(AnnounceContext {
            block_number,
            announcing_peer_id,
            timestamp_ms: now_ms(),
        });
    }

    pub fn begin_own_propagation(&self, tx_hash: String) {
        let mut inner = self.inner.write().expect("poisoned");
        inner.pending_tx_hash = Some(tx_hash);
    }

    pub fn complete_own_propagation(
        &self,
        tx_hash: &str,
        propagate_time_ms: u64,
        propagate_peer_ids: &[String],
        addrs: &std::collections::HashMap<String, String>,
    ) {
        let mut inner = self.inner.write().expect("poisoned");
        let pending = match inner.pending_tx_hash.as_deref() {
            Some(h) if h == tx_hash => inner.pending_tx_hash.take(),
            _ => return,
        };
        let _ = pending;

        let announce = inner.announce.clone();
        let record = OwnPropagationRecord {
            tx_hash: tx_hash.to_string(),
            last_block_number: announce.as_ref().map(|a| a.block_number),
            announced_peer_id: announce.and_then(|a| a.announcing_peer_id),
            timestamp_ms: now_ms(),
            propagate_time_ms,
            propagate_peers: propagate_peer_ids
                .iter()
                .map(|peer_id| PropagationPeerInfo {
                    peer_id: peer_id.clone(),
                    addr: addrs.get(peer_id).cloned(),
                })
                .collect(),
        };

        inner.last = Some(record.clone());
        inner.history.push_back(record);
        while inner.history.len() > HISTORY_CAP {
            inner.history.pop_front();
        }

        log::info!(
            target: "bot::propagation",
            "own propagation tx={tx_hash} block={:?} peers={} time={propagate_time_ms}ms",
            inner.last.as_ref().and_then(|r| r.last_block_number),
            propagate_peer_ids.len(),
        );
    }

    pub fn latest(&self) -> Option<OwnPropagationRecord> {
        self.inner.read().expect("poisoned").last.clone()
    }

    pub fn history(&self, limit: usize) -> Vec<OwnPropagationRecord> {
        let inner = self.inner.read().expect("poisoned");
        inner
            .history
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn enrich_record(
        mut record: OwnPropagationRecord,
        addrs: &std::collections::HashMap<String, String>,
    ) -> OwnPropagationRecord {
        for peer in &mut record.propagate_peers {
            if peer.addr.is_none() {
                peer.addr = addrs.get(&peer.peer_id).cloned();
            }
        }
        record
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
