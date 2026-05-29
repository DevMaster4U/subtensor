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

Three submission strategies are available. Only one is active at a time.

| Mode | RPC | When txs are submitted |
|------|-----|------------------------|
| **Announce** | `bot_startTxs` | Synchronously on block announce (sync hook) + async fallback |
| **Pool front** | `bot_startTxsFront` | Immediately on arm + right after each on-chain inclusion |
| **Hybrid** | `bot_startTxsHybrid` | Pool front **plus** sync announce refresh on every new header |

### Announce mode

Submits inside `BlockAnnounceValidator::validate()` — before async validation and before the broadcast channel — then falls back to the async processor if sync inject fails.

### Pool front mode

Subtensor assigns flat priority `1` to normal EVM transactions. Within that tier the ready pool is **first-come-first-served**. Pool front mode injects early so your transaction sits at the front of the queue before competitors submit on the same block announce.

On block **import**, after your tx is included on-chain, the bot immediately injects the **next** nonce (one tx only) so it reaches proposer mempools before the next block announce.

### Hybrid mode (recommended for FCFS races)

Combines both strategies:

1. **Block announce** — inject / re-propagate the active nonce (once per block).
2. **Block import** — when the tx lands on-chain, advance nonce and **immediately inject the next tx** (closes the ~400–500 ms announce→import gap).
3. **Announce refresh** — keeps the active tx in the pool so competitors cannot jump ahead on the same announce window.

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startTxsHybrid","params":[5],"id":1}' \
  http://127.0.0.1:9944
```

---

## RPC endpoints

All methods are exposed on the node's HTTP JSON-RPC interface.

Base URL used in examples: `http://127.0.0.1:9944`

### `bot_start`

Arm the bot. Does **not** send transactions until a `bot_startTxs*` or `bot_startWithTime` call.

Also starts **announce timing**: each block announce logs its offset within the 12-second wall-clock slot (ms mod 12s), e.g. `36.232s → 232ms`, `49.123s → 1123ms`. Keeps the last **100** blocks.

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

### `bot_startWithTime`

Send `tx_count` transactions on a fixed offset within each 12-second wall-clock slot.

**Params:** `[tx_count: u32, delay_ms: u32]`  
- `delay_ms = 300` → inject at **0.3s, 12.3s, 24.3s, 36.3s, 48.3s**, … (epoch-aligned 12s slots)

**Returns:** `true`

```bash
# 5 txs at 300ms into each 12s slot
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startWithTime","params":[5,300],"id":1}' \
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

### `bot_startTxsHybrid`

Begin sending in **hybrid mode** (pool front + sync announce refresh).

**Params:** `[tx_count: u32]`  
- `tx_count = 0` → unlimited sends while running

**Returns:** `true`

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startTxsHybrid","params":[10],"id":1}' \
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
  "inject_mode": "announce",
  "min_value": 180,
  "average_value": 245.5,
  "schedule_delay_ms": null
}
```

| Field | Description |
|-------|-------------|
| `running` | Whether the bot is armed and may send |
| `tx_remaining` | Sends left in current session; `null` when unlimited |
| `tx_sent` | Total sends completed in current session |
| `inject_mode` | `"announce"`, `"pool_front"`, `"fast"`, or `"scheduled_time"` |
| `min_value` | Min announce offset (ms mod 12s) over last 100 blocks; `null` if none |
| `average_value` | Average announce offset (ms mod 12s) over last 100 blocks |
| `schedule_delay_ms` | Active `bot_startWithTime` offset, or `null` |

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

### `bot_keepTopPeers`

Keep the top N connected peers (by announce score) and disconnect the rest. Writes a JSON log to `filter_log/`.

**Params:** `[keep_count: u32]`

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_keepTopPeers","params":[96],"id":1}' \
  http://127.0.0.1:9944
```

---

### `bot_setReservedPeersFromFile`

Load reserved peers from a file (one multiaddr per line; `#` comments allowed). Registers each peer on the sync peer set (with dial) and on the transactions protocol. Requires a node built with the patched `sc-network-sync` so runtime reserved peers bypass `--in-peers` / `--out-peers` slot limits.

**Params:** `[path: string, clear_all: 0 | 1]`

| `clear_all` | Behavior |
|-------------|----------|
| `0` | Reserved set = file contents. Remove reserved peers not in the file. Other connections stay. |
| `1` | (1) Sever **all** connected peers. (2) Clear **all** reserved peers. (3) Add only peers from the file. |

With `clear_all=1` and a one-line `reserved.txt`, you end up with one reserved peer and no other connections.

```bash
# Reserved set tracks the file; non-reserved connections can stay up
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_setReservedPeersFromFile","params":["/root/subtensor/bot/reserved.txt",0],"id":1}' \
  http://127.0.0.1:9944

# Full reset: drop every peer + every reserved entry, then load the file
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_setReservedPeersFromFile","params":["/root/subtensor/bot/reserved.txt",1],"id":1}' \
  http://127.0.0.1:9944
```

Example `reserved_peers.txt`:

```text
# one multiaddr per line
/ip4/178.105.87.99/tcp/30333/ws/p2p/12D3KooWMbEvnMJKwwwprw4keMMptc3usQcz7gjSrAXoo5QnNzNx
```

---

### `bot_startAutoFilter` / `bot_stopAutoFilter`

Run `bot_keepTopPeers` on a timer. Also configurable via env (`BOT_AUTO_FILTER=1`, `BOT_AUTO_FILTER_INTERVAL`, `BOT_AUTO_FILTER_KEEP`).

```bash
# Keep top 96 peers every 30 minutes
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_startAutoFilter","params":[1800,96],"id":1}' \
  http://127.0.0.1:9944

curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_stopAutoFilter","params":[],"id":1}' \
  http://127.0.0.1:9944
```

---

## Peer control workflow

Typical sequence to trim slow peers and pin fast ones:

1. Run the bot in announce or hybrid mode for a while so `bot_peerStats` accumulates scores.
2. Inspect leaders: `bot_peerStats` / `bot_peerRecommendations`.
3. Prune: `bot_keepTopPeers` (one-shot) or `bot_startAutoFilter` (periodic).
4. Pin the best addresses: write them to a file, then `bot_setReservedPeersFromFile`.

Filter run logs land in `filter_log/` (same format as your existing JSON snapshots).

### Recommended propagation settings

After arming hybrid mode, maximize reach to fast peers:

```bash
# Reserved peers first, then score-ranked peers
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_enableTxPropagationFirstReservedNode","params":[],"id":1}' \
  http://127.0.0.1:9944

# 0 = gossip to all ranked peers (no outbound cap)
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_setTxPropagationMaxPeers","params":[0],"id":1}' \
  http://127.0.0.1:9944
```

### Propagate mode (`bot_setPropagateMode`)

| Mode | Value | Behavior |
|------|-------|----------|
| Normal | `0` | Default ranking, allowlist, `bot_setTxPropagationMaxPeers` |
| Announce | `1` | Gossip only to the peer that announced the latest block |
| Parallel | `2` | Gossip to all connected ranked peers at once (no max cap) |

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_setPropagateMode","params":[1],"id":1}' \
  http://127.0.0.1:9944
```

### Targeted propagation (`bot_propagateToPeers`)

Restrict **all** outbound tx gossip to a fixed peer list (inject, single-tx, and periodic pool gossip):

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_propagateToPeers","params":[["12D3KooW...","/ip4/1.2.3.4/tcp/30333/ws/p2p/12D3KooW..."]],"id":1}' \
  http://127.0.0.1:9944
```

- `peer_ids`: base58 `PeerId` and/or full multiaddr with `/p2p/`.
- Clears the allowlist and restores normal ranking: `params: [[]]`
- Ignores `bot_setTxPropagationMaxPeers` and reserved-first ordering while active.
- Re-gossips the ready pool immediately when a non-empty list is set.
- Active list is visible in `bot_status` → `tx_propagation_peers`.

---

## Finding authorities and fast paths to validators

You cannot map a Substrate `PeerId` to “this slot’s block author” from a non-validator node alone. What you **can** do is find peers that **announce blocks first** and **reliably receive your tx gossip** — those are the best proxies for a low-latency path toward the proposer.

### Step 1 — Run hybrid + collect stats (30+ minutes)

```bash
curl ... bot_startTxsHybrid
curl ... bot_enableTxPropagationFirstReservedNode
curl ... bot_setTxPropagationMaxPeers 0
```

Watch logs for lines like:

```
bot::peers: block #8274993 first announce attributed to 12D3KooW... (N candidates)
```

Peers that win `first announce attributed` often are (or are one hop from) the block producer.

### Step 2 — Inspect leaderboard

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_peerStats","params":[30],"id":1}' \
  http://127.0.0.1:9944

curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_peerRecommendations","params":[15],"id":1}' \
  http://127.0.0.1:9944

curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_checkTxGossip","params":[15],"id":1}' \
  http://127.0.0.1:9944
```

Prioritize peers with **high combined score**:

| Field | Meaning |
|-------|---------|
| `first_announce_hits` | Often saw new block height before others |
| `tx_propagation_hits` | Successfully received your outbound tx gossip |
| `roles` | `AUTHORITY` = validator-capable node; `FULL` = full node relay |
| `addr` | Dialable multiaddr when known |

Prefer peers that score high on **both** announce hits and tx propagation hits.

### Step 3 — Pin reserved peers

Save one multiaddr per line (from `reserved_peer_hint` or `addr`):

```
/ip4/x.x.x.x/tcp/30333/ws/p2p/12D3KooW...
```

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_setReservedPeersFromFile","params":["/path/to/reserved.txt",1],"id":1}' \
  http://127.0.0.1:9944
```

Restart the node with matching `--reserved-nodes` in your systemd/launch flags so connections persist across reboots.

### Step 4 — Prune slow peers

```bash
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_keepTopPeers","params":[96],"id":1}' \
  http://127.0.0.1:9944
```

### On-chain authorities + learned peer mapping (recommended)

Subtensor **removed** `pallet-authority-discovery` — there is no DHT lookup from Aura key → multiaddr.
See [`guid.md`](../guid.md) for the full reference.

The bot implements the practical workflow:

1. **On-chain keys** — query Aura authorities via runtime API  
2. **Next author** — `slot % authority_count` from header digest  
3. **Network addresses** — learn by correlating block author with first announce peer over time  

#### RPC workflow

```bash
# 1. On-chain Aura authorities (sr25519 / AccountId32 hex)
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_auraAuthorities","params":[],"id":1}' \
  http://127.0.0.1:9944

# 2. Current slot + next 5 predicted authors
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_auraSchedule","params":[5],"id":1}' \
  http://127.0.0.1:9944

# 3. Learned { account → peer_id → multiaddr } (grows while node runs)
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_authorityPeers","params":[],"id":1}' \
  http://127.0.0.1:9944

# 4. Connected peers advertising AUTHORITY role
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_connectedAuthorityPeers","params":[],"id":1}' \
  http://127.0.0.1:9944

# 5. Export / apply as reserved peers (min 3 correlation hits)
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_exportAuthorityReserved","params":["/root/subtensor/authority_reserved.txt",3],"id":1}' \
  http://127.0.0.1:9944

curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_applyAuthorityReserved","params":[3],"id":1}' \
  http://127.0.0.1:9944
```

Mappings persist to `authority_peers.json` in the repo root and survive restarts.

#### How learning works

On each block announce the bot:

1. Decodes the **Aura author** from the block header digest  
2. Attributes the **first announce peer** for that block height  
3. Increments `hits` when the same `{author → peer}` pair repeats  

After ~50–100 blocks you should have multiaddrs for most active validators.

Then enable direct propagation:

```bash
curl ... bot_enableTxPropagationFirstReservedNode
curl ... bot_setTxPropagationMaxPeers 0
curl ... bot_startTxsHybrid
```

| Goal | Approach |
|------|----------|
| Know **who** produces blocks | `bot_auraAuthorities` / `bot_auraSchedule` |
| Know **where** they are on P2P | `bot_authorityPeers` (learned over time) |
| Propagate txs directly | `bot_applyAuthorityReserved` + tx propagation settings |
| Guaranteed first position | Run your own authority in the active validator set |

**Note:** `roles: AUTHORITY` on a connected peer means validator-capable node software, not a guaranteed mapping to a specific Aura key. The correlation learner is what bridges that gap.

---

## Running as an authority (validator)

The bot targets **non-validator** full nodes. On an authority node, the block **proposer** picks transaction order from its local pool when building blocks — that beats any pool-front strategy.

To run a local validator (dev/localnet):

```bash
# Local three-node testnet (see scripts/localnet.sh)
./scripts/localnet.sh

# Or single validator manually:
./target/release/node-subtensor \
  --chain local \
  --base-path /tmp/validator \
  --validator \
  --alice \
  --rpc-port 9944
```

Insert session keys if not using a dev preset account:

```bash
./target/release/node-subtensor key insert \
  --base-path /tmp/validator \
  --chain local \
  --scheme Sr25519 \
  --suri "//Alice" \
  --key-type aura

./target/release/node-subtensor key insert \
  --base-path /tmp/validator \
  --chain local \
  --scheme Ed25519 \
  --suri "//Alice" \
  --key-type gran
```

On **Finney/mainnet**, becoming a block author requires being in the active Subtensor validator set (on-chain registration and stake) — not just the `--validator` flag. The flag tells the node to attempt block production when your keys are in the authority set.

---

## Quick reference

| Method | Purpose |
|--------|---------|
| `bot_start` | Arm bot (no sends yet) |
| `bot_stop` | Stop immediately |
| `bot_startTxs` | Send on block announce (sync + fallback) |
| `bot_startTxsFront` | Pre-submit to pool front |
| `bot_startTxsHybrid` | Pool front + sync announce refresh |
| `bot_status` | Running state + counters |
| `bot_peerStats` | Peer announce leaderboard |
| `bot_peerRecommendations` | Suggested `--reserved-peers` |
| `bot_keepTopPeers` | Disconnect peers outside top N |
| `bot_setReservedPeersFromFile` | Pin reserved peers from file |
| `bot_auraAuthorities` | On-chain Aura block producer keys |
| `bot_auraSchedule` | Current slot + predicted next authors |
| `bot_authorityPeers` | Learned Aura account → peer multiaddrs |
| `bot_connectedAuthorityPeers` | Connected AUTHORITY-role peers |
| `bot_exportAuthorityReserved` | Export learned peers to file |
| `bot_applyAuthorityReserved` | Pin learned authority peers as reserved |
| `bot_propagateToPeers` | Restrict all outbound tx gossip to listed peer ids |
| `bot_setPropagateMode` | `0` normal, `1` announce peer only, `2` parallel |
| `bot_propagateMode` | Read current propagate mode |
| `bot_ownPropagationLatest` | Last bot propagation round (send order + addrs) |
| `bot_startAutoFilter` | Periodic peer pruning |
| `bot_stopAutoFilter` | Stop periodic pruning |

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

### Hybrid mode

```bash
curl ... -d '{"jsonrpc":"2.0","method":"bot_startTxsHybrid","params":[10],"id":1}'
curl ... -d '{"jsonrpc":"2.0","method":"bot_status","params":[],"id":1}'
```

`bot_startTxs`, `bot_startTxsFront`, and `bot_startTxsHybrid` all resync the EVM nonce from chain state when called, so you can call them again after a pause without restarting the node.

---

## Architecture

```
Network block announce
  → NotifyingBlockAnnounceValidator::validate()
      → sync_inject (OnAnnounce + Hybrid: submit_local once per block)
      → BlockAnnounceHub broadcast
  → processor loop (OnAnnounce fallback only)
      → peer_tracker.record_announce()

bot_startTxsFront / bot_startTxsHybrid
  → pool_inject loop
      → inject immediately on arm
      → on block import: if tx included → advance nonce → import inject immediately

bot_startTxsHybrid additionally:
  → sync_inject refresh on every announce (re-propagate active nonce)
```

### Crate layout

| Module | Role |
|--------|------|
| `transact.rs` | EIP-1559 signing, prebuild, pool submission |
| `inject_shared.rs` | Shared pending tx state across inject paths |
| `sync_inject.rs` | Synchronous inject from announce validator |
| `processor.rs` | Async announce fallback + peer tracking |
| `pool_inject.rs` | Pool-front early injection |
| `control.rs` | Shared runtime state (running, budget, mode) |
| `rpc.rs` | JSON-RPC control interface |
| `announce.rs` | Block announce notification types |
| `peers.rs` | Peer scoring, pruning, reserved-peer management |
| `mempool.rs` | Optional pool import watcher (debug) |

### Log targets

Filter bot logs:

```bash
RUST_LOG=bot=info,node=info ./target/release/node-subtensor ...
```

| Target | Content |
|--------|---------|
| `bot::transact` | Key load, prebuild, submission |
| `bot::sync_inject` | Sync announce injections |
| `bot::processor` | Async announce fallback |
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
