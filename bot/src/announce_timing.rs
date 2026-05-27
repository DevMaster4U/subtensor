//! Block announce timestamps within each 12-second epoch (mod 12s, in ms).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_BLOCKS: usize = 100;

/// Length of one block slot in milliseconds (Subtensor ~12s).
pub const MOD12_MS: u64 = 12_000;

/// Milliseconds into the current 12-second wall-clock slot (e.g. 36.232s → 232).
pub fn mod12_offset_ms(now: SystemTime) -> u32 {
    let ms = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    (ms % MOD12_MS) as u32
}

pub struct AnnounceTimingTracker {
    enabled: AtomicBool,
    entries: RwLock<VecDeque<(u32, u32)>>,
}

impl Default for AnnounceTimingTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl AnnounceTimingTracker {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            entries: RwLock::new(VecDeque::new()),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    pub fn record(&self, block_number: u32, now: SystemTime) {
        if !self.is_enabled() {
            return;
        }
        let mod12 = mod12_offset_ms(now);
        log::info!(
            target: "bot::timing",
            "block #{block_number} announce mod12={mod12}ms",
        );
        let mut q = self.entries.write().expect("poisoned");
        if q.back().is_some_and(|(n, _)| *n == block_number) {
            return;
        }
        q.push_back((block_number, mod12));
        while q.len() > MAX_BLOCKS {
            q.pop_front();
        }
    }

    pub fn stats(&self) -> (Option<u32>, Option<f64>) {
        let q = self.entries.read().expect("poisoned");
        if q.is_empty() {
            return (None, None);
        }
        let min = q.iter().map(|(_, ms)| *ms).min();
        let sum: u64 = q.iter().map(|(_, ms)| u64::from(*ms)).sum();
        let avg = sum as f64 / q.len() as f64;
        (min, Some(avg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mod12_examples() {
        let t36_232 = UNIX_EPOCH + std::time::Duration::from_millis(36_232);
        assert_eq!(mod12_offset_ms(t36_232), 232);

        let t49_123 = UNIX_EPOCH + std::time::Duration::from_millis(49_123);
        assert_eq!(mod12_offset_ms(t49_123), 1_123);
    }
}
