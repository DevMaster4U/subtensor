//! Runtime toggle between custom (`bot::*`) logs and Substrate default logs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sc_tracing::logging;

/// Switches the global tracing filter between bot-only and system-default output.
pub struct UserLogControl {
    enabled: AtomicBool,
}

impl UserLogControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Substrate default filter with all `bot::*` targets suppressed.
    pub fn apply_system_logs(&self) -> Result<(), String> {
        logging::reset_log_filter()?;
        logging::add_directives("bot=off");
        logging::reload_filter()?;
        self.enabled.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Only `bot::*` targets at info level and above.
    pub fn apply_user_logs(&self) -> Result<(), String> {
        logging::reset_log_filter()?;
        logging::add_directives("off");
        logging::add_directives("bot=info");
        logging::reload_filter()?;
        self.enabled.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn set_enabled(&self, enabled: bool) -> Result<(), String> {
        if enabled {
            self.apply_user_logs()
        } else {
            self.apply_system_logs()
        }
    }
}
