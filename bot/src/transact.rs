//! Transaction building and in-process submission.
//!
//! `TxConfig`  — loaded once from environment variables at startup.
//! `prebuild`  — signs the EIP-1559 tx and converts it to an opaque extrinsic.
//! `send`      — submits the pre-built extrinsic to the pool and triggers immediate P2P gossip.

use fp_rpc::{ConvertTransaction, EthereumRuntimeRPCApi};
use futures::future::BoxFuture;
use k256::ecdsa::{RecoveryId, SigningKey};
use node_subtensor_runtime::{TransactionConverter, opaque::Block};
use pallet_ethereum::Transaction as EthTx;
use crate::propagation_tracker::PropagationTracker;
use sc_network_transactions::TransactionsHandlerController;
use sc_transaction_pool_api::{LocalTransactionPool, TransactionPool};
use sp_core::{H160, U256, keccak_256};
use sp_runtime::traits::Block as BlockT;
use std::sync::Arc;

// ── Config ────────────────────────────────────────────────────────────────────

/// Bot configuration loaded from environment variables.
///
/// Required:
///   BOT_PRIVATE_KEY   — 64-char hex private key (no 0x prefix)
///   BOT_TO            — 40-char hex destination address (no 0x prefix)
///
/// Optional (with defaults):
///   BOT_CHAIN_ID      — default 964
///   BOT_GAS_LIMIT     — default 300000
///   BOT_MAX_FEE       — max_fee_per_gas in wei, default 100_000_000_000
///   BOT_PRIORITY_FEE  — max_priority_fee_per_gas in wei, default 50_000_000_000
pub struct TxConfig {
    pub signing_key:              SigningKey,
    pub from:                     H160,
    pub to:                       H160,
    pub chain_id:                 u64,
    pub gas_limit:                u64,
    pub max_fee_per_gas:          u128,
    pub max_priority_fee_per_gas: u128,
}

/// Load `.env` from the repo root (works regardless of cwd), then read vars.
pub fn load_dotenv() {
    let env_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.env");
    if dotenvy::from_path(&env_path).is_ok() {
        log::debug!(target: "bot::transact", "loaded env from {:?}", env_path);
        return;
    }
    if dotenvy::dotenv().is_ok() {
        log::debug!(target: "bot::transact", "loaded env from cwd .env");
    }
}

impl TxConfig {
    pub fn from_env() -> Self {
        load_dotenv();

        let raw_key = std::env::var("BOT_PRIVATE_KEY")
            .expect("BOT_PRIVATE_KEY env var required (64-char hex, no 0x)");
        let key_bytes = hex::decode(raw_key.trim())
            .expect("BOT_PRIVATE_KEY must be valid hex");
        let signing_key = SigningKey::from_slice(&key_bytes)
            .expect("BOT_PRIVATE_KEY must be a valid 32-byte secp256k1 scalar");

        let from = derive_address(&signing_key);

        let raw_to = std::env::var("BOT_TO")
            .expect("BOT_TO env var required (40-char hex, no 0x)");
        let to_bytes = hex::decode(raw_to.trim())
            .expect("BOT_TO must be valid hex");
        let to = H160::from_slice(&to_bytes);

        let chain_id = std::env::var("BOT_CHAIN_ID")
            .unwrap_or_else(|_| "964".into())
            .parse::<u64>()
            .expect("BOT_CHAIN_ID must be a u64");

        let gas_limit = std::env::var("BOT_GAS_LIMIT")
            .unwrap_or_else(|_| "300000".into())
            .parse::<u64>()
            .expect("BOT_GAS_LIMIT must be a u64");

        let max_fee_per_gas = std::env::var("BOT_MAX_FEE")
            .unwrap_or_else(|_| "100000000000".into())
            .parse::<u128>()
            .expect("BOT_MAX_FEE must be a u128");

        let max_priority_fee_per_gas = std::env::var("BOT_PRIORITY_FEE")
            .unwrap_or_else(|_| "50000000000".into())
            .parse::<u128>()
            .expect("BOT_PRIORITY_FEE must be a u128");

        log::info!(
            target: "bot::transact",
            "🔑 bot address = {:?}  chain_id = {}",
            from, chain_id
        );

        Self { signing_key, from, to, chain_id, gas_limit, max_fee_per_gas, max_priority_fee_per_gas }
    }
}

/// Derive the Ethereum H160 address from a k256 signing key.
pub fn derive_address(key: &SigningKey) -> H160 {
    let point = key.verifying_key().to_encoded_point(false);
    let bytes = point.as_bytes(); // 0x04 || x(32) || y(32)
    let hash = keccak_256(&bytes[1..]);
    H160::from_slice(&hash[12..])
}

// ── PrebuiltTx ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct PrebuiltTx {
    pub extrinsic: <Block as BlockT>::Extrinsic,
}

// ── prebuild ──────────────────────────────────────────────────────────────────

pub fn prebuild(cfg: &TxConfig, nonce: U256, calldata: Vec<u8>) -> PrebuiltTx {
    use ethereum::{
        AccessList, EIP1559Transaction, EIP1559TransactionMessage,
        TransactionAction, eip2930::TransactionSignature,
    };

    let msg = EIP1559TransactionMessage {
        chain_id:                 cfg.chain_id,
        nonce:                    to_et_u256(nonce),
        max_priority_fee_per_gas: ethereum_types::U256::from(cfg.max_priority_fee_per_gas),
        max_fee_per_gas:          ethereum_types::U256::from(cfg.max_fee_per_gas),
        gas_limit:                ethereum_types::U256::from(cfg.gas_limit),
        action:                   TransactionAction::Call(to_et_h160(cfg.to)),
        value:                    ethereum_types::U256::zero(),
        input:                    calldata,
        access_list:              AccessList::new(),
    };

    let hash = msg.hash(); // ethereum_types::H256
    let (odd_y_parity, r, s) = secp256k1_sign(&cfg.signing_key, hash.as_bytes());

    // ethereum 0.18: EIP1559Transaction uses a TransactionSignature struct
    let signature = TransactionSignature::new(odd_y_parity, r, s)
        .expect("valid signature values");

    let tx = EIP1559Transaction {
        chain_id:                 msg.chain_id,
        nonce:                    msg.nonce,
        max_priority_fee_per_gas: msg.max_priority_fee_per_gas,
        max_fee_per_gas:          msg.max_fee_per_gas,
        gas_limit:                msg.gas_limit,
        action:                   msg.action,
        value:                    msg.value,
        input:                    msg.input,
        access_list:              msg.access_list,
        signature,
    };

    let eth_tx = EthTx::EIP1559(tx);
    let extrinsic = TransactionConverter::<Block>::default().convert_transaction(eth_tx);
    log::info!(target: "bot::transact", "✅ tx pre-built, nonce={}", nonce);
    PrebuiltTx { extrinsic }
}

/// Sign a 32-byte hash with k256.
/// Returns (odd_y_parity, r as ethereum_types::H256, s as ethereum_types::H256).
fn secp256k1_sign(
    key: &SigningKey,
    hash: &[u8],
) -> (bool, ethereum_types::H256, ethereum_types::H256) {
    use k256::ecdsa::Signature;

    let (sig, recid): (Signature, RecoveryId) = key
        .sign_prehash_recoverable(hash)
        .expect("signing cannot fail on valid key and 32-byte hash");

    let bytes = sig.to_bytes();
    let r = ethereum_types::H256::from_slice(&bytes[..32]);
    let s = ethereum_types::H256::from_slice(&bytes[32..]);
    (recid.is_y_odd(), r, s)
}

// ── fetch_nonce ───────────────────────────────────────────────────────────────

/// Query the EVM nonce for `address` from the runtime — no HTTP.
pub fn fetch_nonce<C>(client: &C, address: H160) -> Result<U256, String>
where
    C: sp_api::ProvideRuntimeApi<Block> + sp_blockchain::HeaderBackend<Block>,
    C::Api: EthereumRuntimeRPCApi<Block>,
{
    let best_hash = client.info().best_hash;
    let account = client
        .runtime_api()
        .account_basic(best_hash, address)
        .map_err(|e| format!("account_basic runtime call failed: {e:?}"))?;

    Ok(from_et_u256(account.nonce))
}

// ── P2P propagation ───────────────────────────────────────────────────────────

/// Cloneable handle to Substrate's transaction gossip handler.
///
/// Pool submission alone eventually propagates via `on-transaction-imported`, but that task
/// runs asynchronously. Calling [`Self::propagate`] right after a successful pool submit
/// broadcasts the tx on `/transactions/1` immediately.
#[derive(Clone)]
pub struct TxPropagator {
    controller: TransactionsHandlerController<<Block as BlockT>::Hash>,
}

impl TxPropagator {
    pub fn new(controller: TransactionsHandlerController<<Block as BlockT>::Hash>) -> Self {
        Self { controller }
    }

    /// Broadcast a single transaction to connected full-node peers.
    pub fn propagate(&self, hash: <Block as BlockT>::Hash) {
        log::info!(
            target: "bot::transact",
            "📡 P2P propagate hash={:?}",
            hash,
        );
        self.controller.propagate_transaction(hash);
    }
}

// ── send ──────────────────────────────────────────────────────────────────────

fn log_submit_result<P>(
    tx_hash: <P as TransactionPool>::Hash,
    propagator: Option<TxPropagator>,
    result: &Result<<P as TransactionPool>::Hash, <P as TransactionPool>::Error>,
) where
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>,
{
    match result {
        Ok(hash) => {
            log::info!(
                target: "bot::transact",
                "✅ tx submitted to pool, hash={:?}",
                hash,
            );
            if let Some(prop) = propagator {
                prop.propagate(*hash);
            }
        }
        Err(e) => {
            log::error!(
                target: "bot::transact",
                "❌ tx pool submission failed (hash={:?}): {e:?}",
                tx_hash,
            );
        }
    }
}

/// Submit the pre-signed tx to the local pool, then gossip it via P2P immediately.
///
/// Uses [`LocalTransactionPool::submit_local`] (blocking runtime validation on the current
/// thread) instead of the async validation queue in [`TransactionPool::submit_one`].
pub fn send<P>(
    pool: Arc<P>,
    tx: PrebuiltTx,
    best_hash: <Block as BlockT>::Hash,
    propagator: Option<TxPropagator>,
) -> BoxFuture<
    'static,
    Result<
        <P as TransactionPool>::Hash,
        <P as TransactionPool>::Error,
    >,
>
where
    P: TransactionPool<Block = Block, Hash = <Block as BlockT>::Hash>
        + LocalTransactionPool<
            Block = Block,
            Hash = <Block as BlockT>::Hash,
            Error = <P as TransactionPool>::Error,
        >
        + 'static,
{
    let tx_hash = pool.hash_of(&tx.extrinsic);
    Box::pin(async move {
        log::info!(
            target: "bot::transact",
            "📤 submitting tx to pool (hash={:?}, at={:?})",
            tx_hash,
            best_hash,
        );
        let result = pool.submit_local(best_hash, tx.extrinsic);
        log_submit_result::<P>(tx_hash, propagator, &result);
        result
    })
}

// ── type helpers ─────────────────────────────────────────────────────────────

fn to_et_h160(h: H160) -> ethereum_types::H160 {
    ethereum_types::H160::from_slice(h.as_bytes())
}

fn to_et_u256(u: U256) -> ethereum_types::U256 {
    // sp_core::U256 (primitive-types 0.13) — to_big_endian returns [u8;32]
    let bytes: [u8; 32] = u.to_big_endian();
    ethereum_types::U256::from_big_endian(&bytes)
}

fn from_et_u256(u: ethereum_types::U256) -> U256 {
    let bytes = u.to_big_endian();
    U256::from_big_endian(&bytes)
}
