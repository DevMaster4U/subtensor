//! Config file paths (overridable via environment / `config/subtensor.env`).

use std::path::PathBuf;

const ENV_CONFIG_DIR: &str = "SUBTENSOR_CONFIG_DIR";
const ENV_RESERVED_FILE: &str = "SUBTENSOR_RESERVED_FILE";
const ENV_DISABLE_PEERS_FILE: &str = "SUBTENSOR_DISABLE_PEERS_FILE";

/// Config directory (default: `./config` relative to process cwd).
pub fn config_dir() -> PathBuf {
    std::env::var(ENV_CONFIG_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config"))
}

/// Reserved peer multiaddrs file.
pub fn reserved_peers_file() -> PathBuf {
    env_or_join(ENV_RESERVED_FILE, "reserved.txt")
}

/// Disabled peer ids file.
pub fn disable_peers_file() -> PathBuf {
    env_or_join(ENV_DISABLE_PEERS_FILE, "disable_peers.txt")
}

pub fn disable_peers_file_display() -> String {
    disable_peers_file().display().to_string()
}

fn env_or_join(env: &str, name: &str) -> PathBuf {
    std::env::var(env)
        .map(PathBuf::from)
        .unwrap_or_else(|_| config_dir().join(name))
}
