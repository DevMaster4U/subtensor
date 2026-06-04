//! Substrate Node Subtensor CLI library.
#![warn(missing_docs)]

mod bot_tx_inclusion;
mod bot_block_announce {
    pub use node_subtensor::bot_block_announce::*;
}
#[cfg(feature = "runtime-benchmarks")]
mod benchmarking;
mod chain_spec;
mod cli;
mod client;
mod clone_spec;
mod command;
mod conditional_evm_block_import;
mod consensus;
mod ethereum;
mod rpc;
mod service;

fn main() -> sc_cli::Result<()> {
    command::run()
}
