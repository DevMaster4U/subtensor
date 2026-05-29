//! IPC server on the node (bot clients connect via `subtensor-bot`).

mod client_config;
mod manager;

pub use manager::{BlockAnnounceIpcControl, IpcManager, IpcManagerConfig, MempoolIpcControl};
pub use client_config::{AnnounceFilter, ClientConfig};
