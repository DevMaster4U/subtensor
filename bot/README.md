# Subtensor Bot

In-process transaction bot for `node-subtensor`. It pre-builds EIP-1559 Ethereum transactions and submits them directly to the Substrate transaction pool — no HTTP, no external RPC for submission.

The bot is wired into the node at startup (`node/src/service.rs`) and controlled at runtime via JSON-RPC on the same port as the Substrate RPC (typically `9944`).

---

## Build

Build the node (which includes the bot crate):

```bash
cargo build -p node-subtensor --release
```

> **Running on another server?** See **[INTEGRATION.md](./INTEGRATION.md)** for setup from this fork ([DevMaster4U/subtensor](https://github.com/DevMaster4U/subtensor)).

Run the node with RPC enabled:

```bash
./target/release/node-subtensor \
  --chain local \
  --rpc-port 9944 \
  --rpc-cors all \
  --rpc-methods unsafe
```

---

## Configuration

Create a `.env` file in the repository root (or set environment variables). The bot loads `.env` from the repo root automatically.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BOT_PRIVATE_KEY` | yes | — | 64-char hex secp256k1 private key (no `0x` prefix) |
| `BOT_TO` | yes | — | 40-char hex destination EVM address (no `0x` prefix) |
| `BOT_CHAIN_ID` | no | `964` | EIP-1559 chain ID |
| `BOT_GAS_LIMIT` | no | `300000` | Gas limit |
| `BOT_MAX_FEE` | no | `100000000000` | `max_fee_per_gas` in wei |
| `BOT_PRIORITY_FEE` | no | `50000000000` | `max_priority_fee_per_gas` in wei |

Example `.env`:

```env
BOT_PRIVATE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
BOT_TO=0000000000000000000000000000000000000001
BOT_CHAIN_ID=964
```

On startup the bot logs the derived sender address:

```
bot::transact 🔑 bot address = 0x...  chain_id = 964
```

---

## Injection modes

Two submission strategies are available. Only one is active at a time.

| Mode | RPC | When txs are submitted |
|------|-----|------------------------|
| **Announce** | `bot_startTxs` | On pre-import block announce (earliest public network hook) |
| **Pool front** | `bot_startTxsFront` | Immediately on arm + right after each on-chain inclusion |

### Announce mode

Reacts to block headers as soon as the sync engine receives a network block announce — before block download and import complete. This is the fastest hook available on a **non-validator** node.

### Pool front mode

Subtensor assigns flat priority `1` to normal EVM transactions. Within that tier the ready pool is **first-come-first-served**. Pool front mode injects early so your transaction sits at the front of the queue before competitors submit on the same block announce.

---

## RPC endpoints

All methods are exposed on the node's HTTP JSON-RPC interface.

Base URL used in examples: `http://127.0.0.1:9944`

### `bot_start`

Arm the bot. Does **not** send transactions until `bot_startTxs` or `bot_startTxsFront` is called.

**Params:** none  
**Returns:** `true`

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_start","params":[],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_stop`

Stop the bot immediately. No further submissions until re-armed.

**Params:** none  
**Returns:** `true`

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_stop","params":[],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_startTxs`

Begin sending in **announce mode**.

**Params:** `[tx_count: u32]`  
- `tx_count = 0` → unlimited sends while running

**Returns:** `true`

```bash
# Send 5 transactions (one per block announce)
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startTxs","params":[5],"id":1}' \
  http://127.0.0.1:9944

# Unlimited sends
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startTxs","params":[0],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_startTxsFront`

Begin sending in **pool front mode**.

**Params:** `[tx_count: u32]`  
- `tx_count = 0` → unlimited sends while running

**Returns:** `true`

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startTxsFront","params":[5],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_status`

Current bot state.

**Params:** none  
**Returns:**

```json
{
  "running": true,
  "tx_remaining": 3,
  "tx_sent": 2,
  "inject_mode": "announce"
}
```

| Field | Description |
|-------|-------------|
| `running` | Whether the bot is armed and may send |
| `tx_remaining` | Sends left in current session; `null` when unlimited |
| `tx_sent` | Total sends completed in current session |
| `inject_mode` | `"announce"` or `"pool_front"` |

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_status","params":[],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_peerStats`

Leaderboard of peers correlated with early block announces. Useful for researching fast `--reserved-peers`.

**Params:** `[limit?: u32]` — default `20`, clamped to `1`–`200`

**Returns:** array of peer stat objects:

```json
[
  {
    "peer_id": "12D3KooW...",
    "score": 42,
    "first_announce_hits": 7,
    "last_best_number": 1234567,
    "roles": "..."
  }
]
```

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_peerStats","params":[20],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_peerRecommendations`

Top peers to investigate for `--reserved-peers`.

**Params:** `[limit?: u32]` — default `10`, clamped to `1`–`100`

**Returns:** array of recommendation objects:

```json
[
  {
    "peer_id": "12D3KooW...",
    "score": 42,
    "reserved_peer_hint": "..."
  }
]
```

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_peerRecommendations","params":[10],"id":1}' \
  http://127.0.0.1:9944
```

---

## Quick reference

| Method | Purpose |
|--------|---------|
| `bot_start` | Arm bot (no sends yet) |
| `bot_stop` | Stop immediately |
| `bot_startTxs` | Send on block announce |
| `bot_startTxsFront` | Pre-submit to pool front |
| `bot_status` | Running state + counters |
| `bot_peerStats` | Peer announce leaderboard |
| `bot_peerRecommendations` | Suggested `--reserved-peers` |

---

## Typical workflows

### Announce mode

```bash
# 1. Arm
curl ... -d '{"jsonrpc":"2.0","method":"bot_start","params":[],"id":1}'

# 2. Start sending 10 txs on block announces
curl ... -d '{"jsonrpc":"2.0","method":"bot_startTxs","params":[10],"id":1}'

# 3. Monitor
curl ... -d '{"jsonrpc":"2.0","method":"bot_status","params":[],"id":1}'

# 4. Stop early if needed
curl ... -d '{"jsonrpc":"2.0","method":"bot_stop","params":[],"id":1}'
```

### Pool front mode

```bash
# Start immediately (also arms the bot and resyncs nonce)
curl ... -d '{"jsonrpc":"2.0","method":"bot_startTxsFront","params":[10],"id":1}'

curl ... -d '{"jsonrpc":"2.0","method":"bot_status","params":[],"id":1}'
```

`bot_startTxs` and `bot_startTxsFront` both resync the EVM nonce from chain state when called, so you can call them again after a pause without restarting the node.

---

## Architecture

```
Network block announce
  → NotifyingBlockAnnounceValidator (pre-validation notify)
  → BlockAnnounceHub broadcast
  → processor loop
      → peer_tracker.record_announce()
      → [announce mode] pool.submit_one() on new block

bot_startTxsFront
  → pool_inject loop
      → inject immediately on arm
      → re-inject after inclusion (import notification + nonce check)
```

### Crate layout

| Module | Role |
|--------|------|
| `transact.rs` | EIP-1559 signing, prebuild, pool submission |
| `processor.rs` | Block announce listener + announce-mode sends |
| `pool_inject.rs` | Pool-front early injection |
| `control.rs` | Shared runtime state (running, budget, mode) |
| `rpc.rs` | JSON-RPC control interface |
| `announce.rs` | Block announce notification types |
| `peers.rs` | Peer scoring for `--reserved-peers` research |
| `mempool.rs` | Optional pool import watcher (debug) |

### Log targets

Filter bot logs:

```bash
RUST_LOG=bot=info,node=info ./target/release/node-subtensor ...
```

| Target | Content |
|--------|---------|
| `bot::transact` | Key load, prebuild, submission |
| `bot::processor` | Announce-triggered sends |
| `bot::pool_inject` | Pool-front injections |
| `bot::peers` | Peer attribution per block |
| `bot::mempool` | Ready-pool import watcher |

---

## Limitations

- **Non-validator only.** Validator proposer injection is faster but requires authority keys.
- **Flat EVM priority.** Subtensor sets normal EVM tx priority to `1`; gas tips do not reorder the pool. Pool front mode relies on FCFS ordering within that tier.
- **Peer tracking is heuristic.** `BlockAnnounceValidator` does not expose the announcing peer ID; peers are correlated by `best_number` at announce time.
- **`submit_one` always validates.** There is no public API to skip pool validation (~1–10 ms per tx).

---

## Changelog

See [CHANGELOG.md](./CHANGELOG.md) for a commit-by-commit history of bot development.
