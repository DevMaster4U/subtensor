//! Node-wide block announce delivery filter (configured via RPC).

use std::sync::RwLock;

use crate::ipc::AnnounceFilter;

/// Global announce filter applied to all IPC clients.
pub struct AnnounceFilterControl {
    filter: RwLock<AnnounceFilter>,
}

impl Default for AnnounceFilterControl {
    fn default() -> Self {
        Self {
            filter: RwLock::new(AnnounceFilter::default()),
        }
    }
}

impl AnnounceFilterControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, announce_type: &str, value: u64) -> Result<(), String> {
        let filter = AnnounceFilter::from_parts(announce_type, value)?;
        *self.filter.write().expect("poisoned") = filter;
        log::info!(
            target: "bot::announce",
            "announce filter set type={announce_type} value={value}",
        );
        Ok(())
    }

    pub fn get(&self) -> AnnounceFilter {
        *self.filter.read().expect("poisoned")
    }

    pub fn matches(&self, announce_index: u32, delay_time_ms: u64) -> bool {
        self.get().matches(announce_index, delay_time_ms)
    }

    pub fn describe(&self) -> (String, u64) {
        match self.get() {
            AnnounceFilter::Count(n) => ("count".into(), u64::from(n)),
            AnnounceFilter::DelayTime(ms) => ("delay_time".into(), ms),
        }
    }
}
