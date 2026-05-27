//! Runtime control for the bot: start, stop, and transaction bursts.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use tokio::sync::Notify;

/// How the bot submits transactions to the pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InjectMode {
    /// Submit on pre-import block announce (react to new block header).
    OnAnnounce,
    /// Pre-submit and keep the tx in the ready queue (FCFS front position).
    PoolFront,
    /// Pool-front presence plus announce refresh (sync hook + import re-inject).
    Hybrid,
    /// Inject on a fixed offset within each 12-second wall-clock slot.
    ScheduledTime,
}

impl InjectMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::PoolFront,
            2 => Self::Hybrid,
            3 => Self::ScheduledTime,
            _ => Self::OnAnnounce,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::OnAnnounce => 0,
            Self::PoolFront => 1,
            Self::Hybrid => 2,
            Self::ScheduledTime => 3,
        }
    }

    pub fn uses_pool_front(&self) -> bool {
        matches!(self, Self::PoolFront | Self::Hybrid)
    }

    pub fn uses_announce_inject(&self) -> bool {
        matches!(self, Self::OnAnnounce | Self::Hybrid)
    }

    /// Synchronous inject from the block announce validator hook.
    pub fn uses_sync_announce_inject(&self) -> bool {
        matches!(self, Self::OnAnnounce | Self::PoolFront | Self::Hybrid)
    }

    pub fn uses_scheduled_time(&self) -> bool {
        matches!(self, Self::ScheduledTime)
    }
}

/// Shared bot control state, toggled at runtime via RPC.
pub struct BotControl {
    running: AtomicBool,
    /// Remaining sends for the current session. `u32::MAX` means unlimited.
    tx_remaining: AtomicU32,
    tx_sent: AtomicU32,
    needs_nonce_resync: AtomicBool,
    inject_mode: AtomicU8,
    /// Offset within each 12s slot for [`InjectMode::ScheduledTime`] (`u32::MAX` = unset).
    schedule_delay_ms: AtomicU32,
    /// Wakes the pool-front injector on arm/resync without polling.
    pool_wake: Notify,
}

impl Default for BotControl {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(false),
            tx_remaining: AtomicU32::new(0),
            tx_sent: AtomicU32::new(0),
            needs_nonce_resync: AtomicBool::new(false),
            inject_mode: AtomicU8::new(InjectMode::OnAnnounce.as_u8()),
            schedule_delay_ms: AtomicU32::new(u32::MAX),
            pool_wake: Notify::new(),
        }
    }
}

impl BotControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait until [`Self::start_txs_pool_front`] or [`Self::start_txs`] arms the bot.
    pub fn pool_wake(&self) -> tokio::sync::futures::Notified<'_> {
        self.pool_wake.notified()
    }

    /// Arm the bot. Does not send until [`Self::start_txs`] is called.
    /// Enables announce mod12 timing collection.
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.schedule_delay_ms.store(u32::MAX, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Set the send budget and begin submitting on block announces.
    /// `count = 0` means unlimited sends while running.
    pub fn start_txs(&self, count: u32) {
        self.start_txs_with_mode(count, InjectMode::OnAnnounce);
    }

    /// Set the send budget and begin pre-submitting to the pool ready queue.
    /// `count = 0` means unlimited sends while running.
    pub fn start_txs_pool_front(&self, count: u32) {
        self.start_txs_with_mode(count, InjectMode::PoolFront);
    }

    /// Pool-front presence plus synchronous announce refresh on each new header.
    /// `count = 0` means unlimited sends while running.
    pub fn start_txs_hybrid(&self, count: u32) {
        self.start_txs_with_mode(count, InjectMode::Hybrid);
    }

    fn start_txs_with_mode(&self, count: u32, mode: InjectMode) {
        let stored = if count == 0 { u32::MAX } else { count };
        self.tx_remaining.store(stored, Ordering::SeqCst);
        self.inject_mode.store(mode.as_u8(), Ordering::SeqCst);
        if mode != InjectMode::ScheduledTime {
            self.schedule_delay_ms.store(u32::MAX, Ordering::SeqCst);
        }
        self.running.store(true, Ordering::SeqCst);
        self.needs_nonce_resync.store(true, Ordering::SeqCst);
        self.pool_wake.notify_waiters();
    }

    /// Send `count` transactions on a fixed offset within each 12-second wall-clock slot.
    /// `delay_ms = 300` → 0.3s, 12.3s, 24.3s, 36.3s, 48.3s, …
    pub fn start_with_time(&self, count: u32, delay_ms: u32) {
        self.schedule_delay_ms
            .store(delay_ms, Ordering::SeqCst);
        self.start_txs_with_mode(count, InjectMode::ScheduledTime);
    }

    pub fn schedule_delay_ms(&self) -> Option<u32> {
        let v = self.schedule_delay_ms.load(Ordering::SeqCst);
        if v == u32::MAX {
            None
        } else {
            Some(v)
        }
    }

    pub fn inject_mode(&self) -> InjectMode {
        InjectMode::from_u8(self.inject_mode.load(Ordering::SeqCst))
    }

    pub fn tx_remaining(&self) -> Option<u32> {
        let remaining = self.tx_remaining.load(Ordering::SeqCst);
        if remaining == u32::MAX {
            None
        } else {
            Some(remaining)
        }
    }

    pub fn tx_sent(&self) -> u32 {
        self.tx_sent.load(Ordering::SeqCst)
    }

    pub fn should_send(&self) -> bool {
        if !self.running.load(Ordering::SeqCst) {
            return false;
        }
        self.tx_remaining.load(Ordering::SeqCst) > 0
    }

    pub fn take_resync(&self) -> bool {
        self.needs_nonce_resync.swap(false, Ordering::SeqCst)
    }

    pub fn on_sent(&self) {
        self.tx_sent.fetch_add(1, Ordering::SeqCst);
        let remaining = self.tx_remaining.load(Ordering::SeqCst);
        if remaining == u32::MAX {
            return;
        }
        if self.tx_remaining.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.running.store(false, Ordering::SeqCst);
        }
    }
}
