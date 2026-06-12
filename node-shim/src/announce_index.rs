//! Shared per-block announce index (P2P + forwarded RPC share one counter).

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct AnnounceIndexTracker {
    counts: Mutex<HashMap<u32, u32>>,
}

impl AnnounceIndexTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns 1-based announce index for `block_number` and prunes stale heights.
    pub fn next_index(&self, block_number: u32, best_number: u32) -> u32 {
        let mut counts = self.counts.lock().expect("poisoned");
        counts.retain(|&n, _| n > best_number);
        let entry = counts
            .entry(block_number)
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);
        *entry
    }
}
