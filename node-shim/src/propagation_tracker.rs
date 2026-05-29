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
    pending_started_ms: Option<u64>,
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
        inner.pending_started_ms = Some(now_ms());
    }

    /// Hash set by [`Self::begin_own_propagation`] until the next matching completion.
    pub fn pending_own_tx_hash(&self) -> Option<String> {
        self.inner
            .read()
            .expect("poisoned")
            .pending_tx_hash
            .clone()
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
        let started_ms = inner.pending_started_ms.take();

        let announce = inner.announce.clone();
        let record = OwnPropagationRecord {
            tx_hash: tx_hash.to_string(),
            last_block_number: announce.as_ref().map(|a| a.block_number),
            announced_peer_id: announce
                .as_ref()
                .and_then(|a| a.announcing_peer_id.clone()),
            timestamp_ms: started_ms
                .or_else(|| announce.as_ref().map(|a| a.timestamp_ms))
                .unwrap_or_else(now_ms),
            propagate_time_ms,
            propagate_peers: propagate_peer_ids
                .iter()
                .map(|peer_id| PropagationPeerInfo {
                    peer_id: peer_id.clone(),
                    addr: addrs.get(peer_id).cloned(),
                })
                .collect(),
        };

        log_own_propagation(&record);
        inner.last = Some(record.clone());
        inner.history.push_back(record);
        while inner.history.len() > HISTORY_CAP {
            inner.history.pop_front();
        }
    }

    pub fn latest(&self) -> Option<OwnPropagationRecord> {
        self.inner.read().expect("poisoned").last.clone()
    }

    /// Peer id (base58) attributed to the most recent block announce, if known.
    pub fn last_announcing_peer_id(&self) -> Option<String> {
        self.inner
            .read()
            .expect("poisoned")
            .announce
            .as_ref()
            .and_then(|a| a.announcing_peer_id.clone())
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

const LOG_ORDER_PREVIEW: usize = 10;

fn log_own_propagation(record: &OwnPropagationRecord) {
    let order_preview: Vec<String> = record
        .propagate_peers
        .iter()
        .take(LOG_ORDER_PREVIEW)
        .map(|p| match &p.addr {
            Some(addr) => format!("{}@{}", p.peer_id, addr),
            None => p.peer_id.clone(),
        })
        .collect();
    let extra = record.propagate_peers.len().saturating_sub(LOG_ORDER_PREVIEW);
    let order_suffix = if extra > 0 {
        format!(", … +{extra} more")
    } else {
        String::new()
    };

    log::info!(
        target: "bot::propagation",
        "own propagation tx={} block={:?} announcer={:?} peers={} time={}ms send_order=[{}{}]",
        record.tx_hash,
        record.last_block_number,
        record.announced_peer_id,
        record.propagate_peers.len(),
        record.propagate_time_ms,
        order_preview.join(", "),
        order_suffix,
    );
}
