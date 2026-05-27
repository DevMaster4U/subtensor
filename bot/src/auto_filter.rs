//! Periodic peer filtering (same logic as `bot_keepTopPeers` RPC).

use crate::peers::PeerPruner;
use sc_service::TaskManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};
use std::time::Duration;

/// `[interval_secs, keep_count]` auto-filter schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoFilterConfig {
    pub interval_secs: u64,
    pub keep_count: u32,
}

/// Runtime control for the background auto-filter task.
pub struct AutoFilterControl {
    running: AtomicBool,
    interval_secs: AtomicU64,
    keep_count: AtomicU32,
}

impl Default for AutoFilterControl {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(false),
            interval_secs: AtomicU64::new(300),
            keep_count: AtomicU32::new(20),
        }
    }
}

impl AutoFilterControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&self, interval_secs: u64, keep_count: u32) {
        let interval_secs = interval_secs.max(1);
        let keep_count = keep_count.clamp(1, 500);
        self.interval_secs.store(interval_secs, Ordering::SeqCst);
        self.keep_count.store(keep_count, Ordering::SeqCst);
        self.running.store(true, Ordering::SeqCst);
        log::info!(
            target: "bot::auto_filter",
            "auto filter armed: every {interval_secs}s keep top {keep_count}",
        );
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        log::info!(target: "bot::auto_filter", "auto filter stopped");
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs.load(Ordering::SeqCst)
    }

    pub fn keep_count(&self) -> u32 {
        self.keep_count.load(Ordering::SeqCst)
    }

    pub fn config(&self) -> Option<AutoFilterConfig> {
        if !self.is_running() {
            return None;
        }
        Some(AutoFilterConfig {
            interval_secs: self.interval_secs(),
            keep_count: self.keep_count(),
        })
    }
}

/// Parse `BOT_AUTO_FILTER=300,20` or `[300,20]`.
pub fn config_from_env() -> Option<AutoFilterConfig> {
    crate::transact::load_dotenv();

    let raw = std::env::var("BOT_AUTO_FILTER").ok()?;
    let raw = raw.trim();
    let raw = raw.trim_start_matches('[').trim_end_matches(']');
    let mut parts = raw.split(',');
    let interval_secs: u64 = parts
        .next()?
        .trim()
        .trim_end_matches('s')
        .parse()
        .ok()?;
    let keep_count: u32 = parts.next()?.trim().parse().ok()?;
    Some(AutoFilterConfig {
        interval_secs: interval_secs.max(1),
        keep_count: keep_count.clamp(1, 500),
    })
}

pub fn start_auto_filter(
    task_manager: &TaskManager,
    pruner: Arc<PeerPruner>,
    control: Arc<AutoFilterControl>,
    boot: Option<AutoFilterConfig>,
) {
    if let Some(cfg) = boot {
        control.start(cfg.interval_secs, cfg.keep_count);
    }

    task_manager.spawn_handle().spawn(
        "bot-auto-filter",
        None,
        async move {
            loop {
                if !control.is_running() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let every = control.interval_secs();
                let keep = control.keep_count();

                match pruner.keep_top_auto(keep, every).await {
                    Ok(result) => {
                        log::info!(
                            target: "bot::auto_filter",
                            "auto filter: kept {} dropped {} (connected was {})",
                            result.kept_count,
                            result.dropped_count,
                            result.connected_before,
                        );
                    }
                    Err(e) => {
                        log::warn!(target: "bot::auto_filter", "auto filter failed: {e}");
                    }
                }

                tokio::time::sleep(Duration::from_secs(every)).await;
            }
        },
    );
}
