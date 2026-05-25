//! Runtime control for the bot: start, stop, and transaction bursts.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};

/// How the bot submits transactions to the pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InjectMode {
    /// Submit on pre-import block announce (react to new block header).
    OnAnnounce,
    /// Pre-submit and keep the tx in the ready queue (FCFS front position).
    PoolFront,
}

impl InjectMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::PoolFront,
            _ => Self::OnAnnounce,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::OnAnnounce => 0,
            Self::PoolFront => 1,
        }
    }
}

/// Shared bot control state, toggled at runtime via RPC.
#[derive(Debug, Default)]
pub struct BotControl {
    running: AtomicBool,
    /// Remaining sends for the current session. `u32::MAX` means unlimited.
    tx_remaining: AtomicU32,
    tx_sent: AtomicU32,
    needs_nonce_resync: AtomicBool,
    inject_mode: AtomicU8,
}

impl BotControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm the bot. Does not send until [`Self::start_txs`] is called.
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
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

    fn start_txs_with_mode(&self, count: u32, mode: InjectMode) {
        let stored = if count == 0 { u32::MAX } else { count };
        self.tx_remaining.store(stored, Ordering::SeqCst);
        self.inject_mode.store(mode.as_u8(), Ordering::SeqCst);
        self.running.store(true, Ordering::SeqCst);
        self.needs_nonce_resync.store(true, Ordering::SeqCst);
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
