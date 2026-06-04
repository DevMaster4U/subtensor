//! RPC-controlled logging for peer announce timing, propagation delay, and tx inclusion.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};


/// Toggles for optional node metrics logs (enabled via `node_*` RPC).
pub struct MetricsLogControl {
    peer_announce_timing: AtomicBool,
    peer_rtt: AtomicBool,
    tx_inclusion_delay: AtomicBool,
}

impl Default for MetricsLogControl {
    fn default() -> Self {
        Self {
            peer_announce_timing: AtomicBool::new(false),
            peer_rtt: AtomicBool::new(false),
            tx_inclusion_delay: AtomicBool::new(false),
        }
    }
}

impl MetricsLogControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn peer_announce_timing(&self) -> bool {
        self.peer_announce_timing.load(Ordering::SeqCst)
    }

    pub fn set_peer_announce_timing(&self, enabled: bool) {
        self.peer_announce_timing.store(enabled, Ordering::SeqCst);
        log::info!(
            target: "bot::metrics",
            "peer announce timing log {}",
            if enabled { "enabled" } else { "disabled" },
        );
    }

    pub fn peer_rtt(&self) -> bool {
        self.peer_rtt.load(Ordering::SeqCst)
    }

    pub fn set_peer_rtt(&self, enabled: bool) {
        self.peer_rtt.store(enabled, Ordering::SeqCst);
        log::info!(
            target: "bot::metrics",
            "peer libp2p ping RTT log {}",
            if enabled { "enabled" } else { "disabled" },
        );
    }

    pub fn tx_inclusion_delay(&self) -> bool {
        self.tx_inclusion_delay.load(Ordering::SeqCst)
    }

    pub fn set_tx_inclusion_delay(&self, enabled: bool) {
        self.tx_inclusion_delay.store(enabled, Ordering::SeqCst);
        log::info!(
            target: "bot::metrics",
            "tx inclusion delay log {}",
            if enabled { "enabled" } else { "disabled" },
        );
    }
}

/// Pending bot txs awaiting on-chain inclusion.
#[derive(Default)]
pub struct TxInclusionTracker {
    pending: RwLock<HashMap<String, u64>>,
}

impl TxInclusionTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_submitted(&self, tx_hash: String) {
        let submitted_ms = now_ms();
        self.pending
            .write()
            .expect("poisoned")
            .insert(tx_hash, submitted_ms);
    }

    pub fn pending_snapshot(&self) -> HashMap<String, u64> {
        self.pending.read().expect("poisoned").clone()
    }

    pub fn take_pending(&self, tx_hash: &str) -> Option<u64> {
        self.pending
            .write()
            .expect("poisoned")
            .remove(tx_hash)
    }
}

pub fn log_peer_announce_timing(
    control: &MetricsLogControl,
    block_number: u32,
    peer_id: &str,
    announce_index: u32,
    delay_time_ms: u64,
) {
    if !control.peer_announce_timing() {
        return;
    }
    log::info!(
        target: "bot::metrics",
        "peer_announce_timing block={block_number} peer={peer_id} index={announce_index} delay_time_ms={delay_time_ms}",
    );
}

pub fn log_tx_inclusion_delay(
    control: &MetricsLogControl,
    tx_hash: &str,
    block_number: u32,
    submitted_ms: u64,
) {
    if !control.tx_inclusion_delay() {
        return;
    }
    let inclusion_ms = now_ms().saturating_sub(submitted_ms);
    log::info!(
        target: "bot::metrics",
        "tx_inclusion_delay tx={tx_hash} block={block_number} delay_ms={inclusion_ms}",
    );
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
