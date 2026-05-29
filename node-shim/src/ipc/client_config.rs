//! Per-IPC-client settings (announce filter, mempool, peer-find).

/// Per-client announce delivery filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnounceFilter {
    /// Deliver the first `n` announces per block (`0` = all).
    Count(u32),
    /// Deliver when slot delay (ms within 12s cycle) equals `ms`.
    DelayTime(u64),
}

impl Default for AnnounceFilter {
    fn default() -> Self {
        Self::Count(1)
    }
}

impl AnnounceFilter {
    pub fn from_parts(announce_type: &str, value: u64) -> Result<Self, String> {
        match announce_type {
            "count" => Ok(Self::Count(value.min(u32::MAX as u64) as u32)),
            "delay_time" => Ok(Self::DelayTime(value)),
            other => Err(format!(
                "announce_type must be \"count\" or \"delay_time\", got \"{other}\""
            )),
        }
    }

    pub fn matches(&self, announce_index: u32, delay_time_ms: u64) -> bool {
        match self {
            Self::Count(0) => true,
            Self::Count(n) => announce_index <= *n,
            Self::DelayTime(v) => delay_time_ms == *v,
        }
    }
}

/// Settings stored per connected bot client.
#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    pub announce_filter: AnnounceFilter,
    pub require_mempool: bool,
    pub require_peer_find: bool,
}

impl ClientConfig {
    pub fn set_announce(&mut self, announce_type: &str, value: u64) -> Result<(), String> {
        self.announce_filter = AnnounceFilter::from_parts(announce_type, value)?;
        Ok(())
    }
}
