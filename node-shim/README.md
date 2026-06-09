# subtensor-node-shim

Node-side control layer for the Subtensor bot stack:

- **Unix socket IPC** — external bots connect and receive events / submit transactions
- **`node_*` JSON-RPC** — operators enable IPC forwarding, manage peers, and tune global tx gossip
- **Per-client IPC settings** — each bot connection configures its own announce filter, mempool opt-in, and per-submit propagation

Used by the [subtensor](https://github.com/opentensor/subtensor) node binary. Bots use [`subtensor-bot`](https://github.com/DevMaster4U/subtensor-bot) and the shared [`subtensor-ipc`](../ipc) protocol crate.

## Architecture

```
                    ┌─────────────────────────────────────┐
  Bot client        │  node-subtensor                     │
  (subtensor-bot)   │                                     │
       │            │  ┌──────────────┐  ┌─────────────┐ │
       │  Unix IPC  │  │ IpcManager   │  │ NodeControl │ │
       └───────────►│  │ (per-client │  │ Rpc (node_*)│ │
                    │  │  settings)   │  │ global)     │ │
                    │  └──────┬───────┘  └──────┬──────┘ │
                    │         │                  │       │
                    │         ▼                  ▼       │
                    │  block announce /    peer manager  │
                    │  mempool / tx pool / tx gossip     │
                    └─────────────────────────────────────┘
```

**Two control planes:**

| Plane | Who uses it | Scope |
|-------|-------------|--------|
| **JSON-RPC `node_*`** | Operator / automation via HTTP WS (port 9944) | **Global** node toggles (enable IPC, peer dial, default propagate mode) |
| **IPC messages** | Bot process on the socket | **Per-connection** settings (announce filter, mempool opt-in, per-tx propagate) |

Both are required for full operation: RPC turns on forwarding; IPC configures what each bot receives and how each submit propagates.

---

## Socket and environment

| Item | Default |
|------|---------|
| Socket path | `/tmp/subtensor-bot.sock` |
| Override | `SUBTENSOR_IPC_PATH` |

Wire format: **4-byte big-endian length** + **JSON** body (see `subtensor-ipc`).

---

## IPC protocol

### Node → bot (events)

The node only sends these after the matching **global RPC toggle** is enabled (see [IPC global toggles](#ipc-global-toggles-rpc)).

#### `header`

Block announce for the **next** block (`best + 1`).

```json
{
  "type": "header",
  "header_number": 12345,
  "hash": "0x…",
  "parent_hash": "0x…",
  "slot": 7,
  "announcing_peer": "12D3Koo…",
  "announce_index": 1,
  "delay_time_ms": 983
}
```

| Field | Meaning |
|-------|---------|
| `announce_index` | 1-based announce counter for this block height on this node (1 = first seen) |
| `delay_time_ms` | Milliseconds within the 12-second wall-clock cycle: `(unix_secs % 12) * 1000` rounded. Examples: elapsed `12.983` → `983`; `13.23` → `1230` |

Delivery to IPC clients is filtered by the node-wide **`node_setAnnounceFilter`** RPC setting (see [Announce filter](#announce-filter-rpc)).

#### `mempool`

```json
{
  "type": "mempool",
  "info": "{\"tx_hash\":\"0x…\",\"extrinsic\":\"0x…\"}"
}
```

`info` is a JSON **string** containing `tx_hash` and hex `extrinsic`.

Only sent to clients with **`set_require_mempool: true`**.

Requires: `node_enableMempoolWatcher` + `node_enableMempoolIpc`.

#### `find_peer`

```json
{
  "type": "find_peer",
  "peer_id": "12D3Koo…",
  "multiaddr": "/ip4/…/tcp/…/p2p/…"
}
```

Emitted when **peer logging** is on (`node_enableLogPeer`) and a **new** peer is seen (not a repeat connection). Only sent to clients with **`set_require_peer_find: true`**.

---

### Bot → node (commands)

#### Announce filter (RPC)

IPC `set_announce` is **deprecated**; configure the filter on the node via JSON-RPC:

| RPC | Params | Effect |
|-----|--------|--------|
| `node_setAnnounceFilter` | `announce_type`, `value` | Set global filter |
| `node_announceFilter` | — | Read current filter |

| `announce_type` | `value` | Effect |
|-----------------|---------|--------|
| `"count"` | `0` | All announces for each block |
| `"count"` | `N` | First `N` announces per block (`1` = first announce only) |
| `"delay_time"` | `ms` | Only announces where `delay_time_ms == value` |

**Default:** `count` / `1`.

```bash
curl -s localhost:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setAnnounceFilter","params":["count",9],"id":1}'
```

#### `set_require_mempool` — opt in to mempool events

```json
{ "type": "set_require_mempool", "enabled": true }
```

Default: `false`. Must be `true` for that client to receive `mempool` frames.

#### `set_require_peer_find` — opt in to peer discovery events

```json
{ "type": "set_require_peer_find", "enabled": true }
```

Default: `false`. Requires node `node_enableLogPeer` as well.

#### `transaction` — submit extrinsic + optional propagation

**Fast path (recommended):** client validates SCALE on the bot, sends inner payload:

```json
{
  "type": "transaction",
  "extrinsic": "0x…",
  "propagate_type": "announce",
  "propagate_param": "3"
}
```

| Field | Meaning |
|-------|---------|
| `extrinsic` | Hex of **inner** opaque extrinsic bytes (pre-validated on client). Node hex-decodes only — no full SCALE validate on hot path. |
| `hash` | Legacy: full wire hex (`0x` + SCALE opaque extrinsic). Node hex-decodes **and** SCALE-decodes. |
| `propagate_type` | Per-submit gossip override (optional). See [Per-submit propagation](#per-submit-propagation-ipc). |
| `propagate_param` | Second argument for `propagate_type` |

Rust:

```rust
// Fast path
let msg = IpcClient::transaction_prepared("0x…wire_hex…")?;
let msg = IpcClient::transaction_prepared_with_propagate("0x…", "normal", "avg_announce_time")?;

// Legacy
let msg = IpcClient::transaction("0x…wire_hex…".into());
```

Preparation on client (`prepare_extrinsic_hex`): hex-decode wire → SCALE-validate → extract inner payload → send as `extrinsic`.

---

## Per-client IPC settings

Stored **per connected socket** in `ClientConfig`:

| Setting | IPC method | Default |
|---------|------------|---------|
| Announce filter | `node_setAnnounceFilter` RPC | `count` / `1` |
| Mempool notifications | `set_require_mempool` | `false` |
| Peer-find notifications | `set_require_peer_find` | `false` |
| Per-submit propagation | `transaction.propagate_*` | none (uses global `TxPropagationControl`) |

Multiple bots can connect with different filters on the same node.

**Typical bot startup sequence:**

```rust
// set announce filter via node_setAnnounceFilter RPC before connecting IPC
outgoing.send(IpcClient::set_require_mempool(true))?;
outgoing.send(IpcClient::set_require_peer_find(true))?;
```

---

## Per-submit propagation (IPC)

When `propagate_type` is set on a `transaction` message, it overrides global mode **for that submit only** (via `TxPropagationRequest` + `BotPeerRanker`).

| `propagate_type` | `propagate_param` | Behavior |
|------------------|-------------------|----------|
| `"normal"` | `"avg_announce_time"` | Rank connected tx peers by lowest average announce delay; gossip to ranked set |
| `"normal"` | `"first_announce_hit_count"` | Rank by first-announce hit count (default if name omitted) |
| `"announce"` | `"N"` (count) | Parallel gossip to first **N** peers from current block announce order |
| `"parallel"` or `"parrel"` | (ignored) | Parallel gossip to all connected tx peers |

Global RPC settings (`node_setPropagateMode`, allowlist, max peers, reserved-first) still apply when IPC does not set a per-submit override.

---

## IPC global toggles (RPC)

These are **node-wide**; bots cannot change them over IPC.

| RPC method | Purpose |
|------------|---------|
| `node_enableBlockAnnounceIpc` | Allow `header` events to be forwarded to IPC clients |
| `node_disableBlockAnnounceIpc` | Stop header forwarding |
| `node_enableMempoolWatcher` | Start watching the local tx pool import stream |
| `node_disableMempoolWatcher` | Stop watcher |
| `node_enableMempoolLog` | Log each pool import + ready-queue order (`bot::pool`, enabled by default) |
| `node_disableMempoolLog` | Stop mempool/pool import logging |
| `node_enablePoolImportLog` | Alias for `node_enableMempoolLog` |
| `node_disablePoolImportLog` | Alias for `node_disableMempoolLog` |
| `node_enableMempoolIpc` | Allow mempool events to reach IPC (still per-client opt-in) |
| `node_disableMempoolIpc` | Stop mempool IPC |
| `node_setAnnounceFilter` | Global IPC header announce filter (`count` or `delay_time`) |
| `node_announceFilter` | Read announce filter |
| `node_enablePeerAnnounceTimingLog` | Log per-peer `delay_time_ms` on each block announce |
| `node_disablePeerAnnounceTimingLog` | Disable announce timing logs |
| `node_enablePeerRttLog` | Log per-peer libp2p ping RTT (`/ipfs/ping/1.0.0`, `rtt_ms`) |
| `node_disablePeerRttLog` | Disable ping RTT logs |
| `node_enableTxInclusionDelayLog` | Log submit→block inclusion delay for bot-submitted txs |
| `node_disableTxInclusionDelayLog` | Disable inclusion delay logs |
| `node_enableLogPeer` | Log newly seen peers; enables `find_peer` IPC when clients opt in |
| `node_disableLogPeer` | Disable peer logging |

**Minimal enable example:**

```bash
curl -s localhost:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableBlockAnnounceIpc","params":[],"id":1}'

curl -s localhost:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableMempoolWatcher","params":[],"id":1}'

curl -s localhost:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableMempoolIpc","params":[],"id":1}'
```

---

## Node JSON-RPC reference (`node_*`)

All methods are on the node's standard JSON-RPC endpoint (e.g. `ws://127.0.0.1:9944`).

### Status

#### `node_status` / `node_ipcStatus`

Returns global IPC and propagation snapshot.

```json
{
  "socket_path": "/tmp/subtensor-bot.sock",
  "block_announce_ipc": true,
  "mempool_ipc": true,
  "mempool_watcher": true,
  "mempool_log": true,
  "pool_import_log": true,
  "propagate_mode": 0,
  "propagate_mode_label": "normal",
  "tx_propagation_first_reserved_node": true,
  "tx_propagation_max_peers": 0,
  "tx_propagation_peers": null,
  "announce_filter_type": "count",
  "announce_filter_value": 1,
  "log_peer_announce_timing": false,
  "log_peer_rtt": false,
  "log_tx_inclusion_delay": false
}
```

---

### Peer management

| Method | Params | Returns | Description |
|--------|--------|---------|-------------|
| `node_peerConnect` | `multiaddr: string` | `peer_id` | Dial a peer |
| `node_peerDisconnect` | `peer_id: string` | `bool` | Disconnect one peer |
| `node_peerDisconnectAll` | — | `u32` | Disconnect count |
| `node_peerConnectFromFile` | `path: string` | `{ loaded, peer_ids, multiaddrs }` | Add reserved peers from file (does not disconnect existing peers) |
| `node_clearNormalPeers` | — | `{ disconnected }` | Disconnect non-reserved peers |
| `node_enableNormalPeer` | — | `true` | Allow inbound discovered peers |
| `node_disableNormalPeer` | — | `{ disconnected }` | Deny + disconnect normal peers |
| `node_peerSetMode` | `mode: u8` | `mode` | `0` only_custom, `1` both, `2` only_system |
| `node_peerSetCheckingTime` | `checking_ms, sleep_ms` | `true` | Custom peer dial loop timing |
| `node_peerStatus` | — | `PeerManageStatus` | Full peer manager snapshot |
| `node_peerList` | — | `[PeerListEntry]` | Each connected peer: `peer_id`, `multiaddr`, `role`, plus `sync` / `tx_reserved` / `custom` / `reserved` flags |
| `node_peerScores` | — | `PeerScoreboardExport` | Per-peer racing metrics + composite score (see below) |

#### `node_peerScores` — peer ranking for trading/racing

Returns all tracked peers (plus currently connected sync peers), sorted by composite `score` descending.

Per peer:

| Field | Meaning |
|-------|---------|
| `rtt_ms` | Latest libp2p ping RTT |
| `avg_rtt_ms` | Average ping RTT |
| `blocks_received_first` | Blocks where this peer was the first announcer |
| `first_block_percentage` | `blocks_received_first / total_blocks` |
| `avg_block_announcement_delay_ms` | Average slot delay (`delay_time_ms`) on announces from this peer |
| `disconnect_count` | Sync disconnect events |
| `connect_count` | Sync connect events |
| `rtt_score` | Normalized RTT component (lower RTT → higher, best peer = 1.0) |
| `uptime_score` | Connection stability (connect vs disconnect ratio) |
| `score` | `0.6 × first_block_percentage + 0.3 × rtt_score + 0.1 × uptime_score` |

```bash
curl -s localhost:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerScores","params":[],"id":1}'
```

Peer logging is **enabled automatically** at node startup (`peer_log.txt`). `node_enableLogPeer` accepts optional `path` (default `peer_log.txt`) to override or re-enable. When enabled, new peers are appended to the log file and eligible clients may receive `find_peer` IPC events.

---

### Transaction propagation (global)

Controls default gossip for pool submits (including IPC) when no per-submit IPC override is active. Implemented via patched `sc-network-transactions` + `BotPeerRanker`.

#### `node_setPropagateMode` / `node_propagateMode`

| Mode | Value | Label | Behavior |
|------|-------|-------|----------|
| Normal | `0` | `normal` | Rank peers (allowlist / reserved-first / max peers) |
| Announce first | `1` | `announce` | Gossip only to last block announcer |
| Parallel | `2` | `parallel` | Gossip to all tx peers in parallel |

#### `node_enableTxPropagationFirstReservedNode` / `node_disableTxPropagationFirstReservedNode`

When enabled, `--reserved-nodes` are ordered first in normal-mode ranking.

#### `node_setTxPropagationMaxPeers`

`max: u32` — cap outbound peers per round in normal mode. `0` = no limit.

#### `node_propagateToPeers`

`peer_ids: string[]` — restrict **all** outbound gossip to this allowlist (base58 or `/p2p/…` multiaddr). Empty array clears allowlist.

Returns:

```json
{
  "enabled": true,
  "peers": ["12D3Koo…"],
  "invalid_peer_ids": []
}
```

#### `node_ownPropagationLatest` / `node_ownPropagationHistory`

Diagnostics for bot-initiated propagations (correlated with block announces).

`OwnPropagationRecord`:

| Field | Meaning |
|-------|---------|
| `tx_hash` | Submitted transaction |
| `last_block_number` | Block context at submit |
| `announced_peer_id` | Attributed announcer |
| `propagate_time_ms` | Gossip round duration |
| `propagate_peers` | `{ peer_id, addr }` send order |

`node_ownPropagationHistory` accepts optional `limit` (default 20, max 100).

---

## End-to-end setup

### 1. Operator (RPC) — enable forwarding

```bash
# Block announces → IPC
node_enableBlockAnnounceIpc

# Mempool → IPC
node_enableMempoolWatcher
node_enableMempoolIpc

# Optional: peer discovery log + find_peer IPC
node_enableLogPeer
```

### 2. Bot (IPC) — per-client settings

```rust
outgoing.send(IpcClient::set_announce("count", 0))?;
outgoing.send(IpcClient::set_require_mempool(true))?;
outgoing.send(IpcClient::set_require_peer_find(true))?;
```

### 3. Bot — submit transaction (fast path)

```rust
let msg = IpcClient::transaction_prepared_with_propagate(
    &signed_extrinsic_hex,
    "announce",
    "3",
)?;
outgoing.send(msg)?;
```

### 4. Operator (optional) — global propagation defaults

```bash
node_setPropagateMode 0          # normal
node_enableTxPropagationFirstReservedNode
node_setTxPropagationMaxPeers 5
```

---

## Dependency

`subtensor-ipc` lives in this repo at `ipc/` and is a workspace dependency:

```toml
subtensor-ipc = { workspace = true }
```

Tx gossip ranking uses a vendored patch:

```toml
[patch."https://github.com/opentensor/polkadot-sdk.git"]
sc-network-transactions = { path = "vendor/sc-network-transactions" }
```

See `vendor/sc-network-transactions/` for `PeerRanker` / parallel dispatch support.
