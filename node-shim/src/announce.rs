//! Block announce helpers for the node hook and IPC events.

use std::time::{SystemTime, UNIX_EPOCH};

use sp_consensus_aura::AURA_ENGINE_ID;
use sp_runtime::generic::DigestItem;
use sp_runtime::traits::Header as HeaderT;

/// Returns true when `header` is the immediate next block after local best.
pub fn is_immediate_next_block<H>(header: &H, best_number: u32) -> bool
where
    H: HeaderT<Number = u32>,
{
    *header.number() == best_number.saturating_add(1)
}

/// Aura slot from a pre-import header digest.
pub fn slot_from_digest(digest: &sp_runtime::Digest) -> Option<u64> {
    for log in digest.logs() {
        if let DigestItem::PreRuntime(engine, data) = log {
            if engine == &AURA_ENGINE_ID {
                return codec::Decode::decode(&mut &data[..]).ok();
            }
        }
    }
    None
}

/// Milliseconds within the 12-second slot cycle from wall-clock elapsed seconds.
///
/// Examples: `12.983` → `983`, `13.23` → `1230`.
pub fn delay_time_ms_from_elapsed_secs(elapsed_secs: f64) -> u64 {
    let remainder = elapsed_secs % 12.0;
    (remainder * 1000.0).round() as u64
}

/// Current wall-clock delay time within the 12-second cycle.
pub fn current_delay_time_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    delay_time_ms_from_elapsed_secs(elapsed)
}
