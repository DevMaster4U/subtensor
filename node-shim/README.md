# subtensor-node-shim

Node-side control layer for the Subtensor bot stack:

- **Unix socket IPC** — external bots connect and receive events / submit transactions
- **`node_*` JSON-RPC** — operators enable IPC forwarding, manage peers, and tune global tx gossip
- **Per-client IPC settings** — each bot connection opts in to mempool / peer-find events and per-submit propagation

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
| **JSON-RPC `node_*`** | Operator / automation via HTTP WS (port 9944) | **Global** node toggles |
| **IPC messages** | Bot process on the socket | **Per-connection** opt-in + per-tx propagate |

All custom bot features are **disabled at startup**. Enable each via `node_*` RPC before expecting bot behaviour.

---

## Socket and environment

| Item | Default |
|------|---------|
| Socket path | `/tmp/subtensor-bot.sock` (only after `node_startIpc`) |
| Override | `SUBTENSOR_IPC_PATH` on bot side |
| Reserved peers file | `config/reserved.txt` (preloaded at startup) |
| Disabled peers file | `config/disable_peers.txt` (preloaded at startup) |

Wire format: **4-byte big-endian length** + **JSON** body (see `subtensor-ipc`).

---

## IPC protocol

### Node → bot (events)

Requires matching **global RPC toggle** + client opt-in where noted.

#### `header`

Block announce for the **next** block (`best + 1`). Requires `node_enableBlockAnnounceIpc`. Filtered by `node_setAnnounceFilter`.

```json
{
  "type": "header",
  "header_number": 8348334,
  "hash": "0xabc…",
  "parent_hash": "0xdef…",
  "slot": 148427940,
  "announcing_peer": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
  "announce_index": 1,
  "delay_time_ms": 983
}
```

| Field | Meaning |
|-------|---------|
| `announce_index` | 1-based announce counter for this block height (1 = first seen) |
| `delay_time_ms` | ms within 12 s wall-clock cycle: `(unix_secs % 12) * 1000` |

#### `mempool`

Requires `node_enableMempoolWatcher` + `node_enableMempoolIpc` + client `set_require_mempool: true`.

```json
{
  "type": "mempool",
  "info": "{\"tx_hash\":\"0x1a2b…\",\"extrinsic\":\"0x28…\"}"
}
```

#### `find_peer`

Requires `node_enableLogPeer` + client `set_require_peer_find: true`.

```json
{
  "type": "find_peer",
  "peer_id": "12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt",
  "multiaddr": "/ip4/88.216.36.114/tcp/30333/p2p/12D3Koo…"
}
```

### Bot → node (commands)

| Message | Purpose |
|---------|---------|
| `set_require_mempool` | Opt in to `mempool` frames (default `false`) |
| `set_require_peer_find` | Opt in to `find_peer` frames (default `false`) |
| `transaction` | Submit extrinsic + optional per-submit propagation |

Announce filtering is **node-wide** via `node_setAnnounceFilter` (IPC `set_announce` is deprecated).

**Fast-path transaction submit:**

```json
{
  "type": "transaction",
  "extrinsic": "0x…inner_opaque_hex…",
  "propagate_type": "announce",
  "propagate_param": "3"
}
```

| `propagate_type` | `propagate_param` | Behavior |
|------------------|-------------------|----------|
| `"normal"` | `"avg_announce_time"` | Rank tx peers by lowest average announce delay |
| `"normal"` | `"first_announce_hit_count"` | Rank by first-announce hit count (default name) |
| `"announce"` | `"N"` | Parallel gossip to first N block announcers |
| `"parallel"` | (ignored) | Parallel gossip to all connected tx peers |

---

## Node JSON-RPC reference (`node_*`)

**Endpoint:** `http://127.0.0.1:9944` (or your node's HTTP RPC port)

**Request format:**

```bash
curl -s http://127.0.0.1:9944 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"<METHOD>","params":[...],"id":1}'
```

**Response format:**

```json
{"jsonrpc":"2.0","id":1,"result": ... }
```

### Method index

| Method | Params | Returns |
|--------|--------|---------|
| `node_status` | — | `NodeStatus` |
| `node_ipcStatus` | — | `NodeStatus` (alias) |
| `node_startIpc` | `path: string` | `string` (bound path) |
| `node_enableBlockAnnounceIpc` | — | `bool` |
| `node_disableBlockAnnounceIpc` | — | `bool` |
| `node_setAnnounceFilter` | `announce_type, value` | `bool` |
| `node_getAnnounceFilter` | — | `[string, u64]` |
| `node_enableMempoolWatcher` | — | `bool` |
| `node_disableMempoolWatcher` | — | `bool` |
| `node_enableMempoolIpc` | — | `bool` |
| `node_disableMempoolIpc` | — | `bool` |
| `node_enableMempoolLog` | — | `bool` |
| `node_disableMempoolLog` | — | `bool` |
| `node_enableUserLog` | — | `bool` |
| `node_disableUserLog` | — | `bool` |
| `node_enablePeerAnnounceTimingLog` | — | `bool` |
| `node_disablePeerAnnounceTimingLog` | — | `bool` |
| `node_enablePeerRttLog` | — | `bool` |
| `node_disablePeerRttLog` | — | `bool` |
| `node_enableTxInclusionDelayLog` | — | `bool` |
| `node_disableTxInclusionDelayLog` | — | `bool` |
| `node_peerConnect` | `multiaddr: string` | `string` (peer_id) |
| `node_peerDisconnect` | `peer_id: string` | `bool` |
| `node_peerDisconnectAll` | — | `u32` |
| `node_peerConnectFromFile` | `path: string` | `ConnectFileResult` |
| `node_clearNormalPeers` | — | `ClearNormalPeersResult` |
| `node_enableNormalPeer` | — | `bool` |
| `node_disableNormalPeer` | — | `ClearNormalPeersResult` |
| `node_enableLogPeer` | `path?: string` | `bool` |
| `node_disableLogPeer` | — | `bool` |
| `node_peerSetMode` | `mode: u8` | `u8` |
| `node_peerSetCheckingTime` | `checking_ms, sleep_ms` | `bool` |
| `node_peerStatus` | — | `PeerManageStatus` |
| `node_peerList` | — | `[PeerListEntry]` |
| `node_setDisablePeers` | `peer_ids: string[]` | `SetDisablePeersResult` |
| `node_setDisablePeersFromFile` | `path: string` | `SetDisablePeersResult` |
| `node_peerFindClosest` | `peer_id: string` | `FindClosestPeersResult` |
| `node_peerScores` | — | `PeerScoreboardExport` |
| `node_slotState` | — | `SlotStateExport` |
| `node_slotStateBySlot` | `slot: u32` (0–19) | `SlotState` |
| `node_setPropagateMode` | `mode: u8` | `u8` |
| `node_propagateMode` | — | `u8` |
| `node_enableTxPropagationFirstReservedNode` | — | `bool` |
| `node_disableTxPropagationFirstReservedNode` | — | `bool` |
| `node_setTxPropagationMaxPeers` | `max: u32` | `bool` |
| `node_propagateToPeers` | `peer_ids: string[]` | `SetPropagationPeersResult` |
| `node_ownPropagationLatest` | — | `OwnPropagationRecord \| null` |
| `node_ownPropagationHistory` | `limit?: u32` | `[OwnPropagationRecord]` |

---

### Status & IPC

#### `node_status` / `node_ipcStatus`

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_status","params":[],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "socket_path": "/tmp/subtensor-bot.sock",
    "ipc_listening": true,
    "block_announce_ipc": true,
    "mempool_ipc": false,
    "mempool_watcher": false,
    "mempool_log": false,
    "propagate_mode": 0,
    "propagate_mode_label": "normal",
    "tx_propagation_first_reserved_node": false,
    "tx_propagation_max_peers": 0,
    "tx_propagation_peers": null,
    "announce_filter_type": "count",
    "announce_filter_value": 1,
    "log_peer_announce_timing": false,
    "log_peer_rtt": false,
    "log_tx_inclusion_delay": false,
    "user_log": false
  }
}
```

#### `node_startIpc`

Start or restart the Unix socket listener. **Required before bots can connect.**

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_startIpc","params":["/tmp/subtensor-bot.sock"],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":"/tmp/subtensor-bot.sock"}
```

---

### Block announce IPC

#### `node_enableBlockAnnounceIpc` / `node_disableBlockAnnounceIpc`

Enable/disable forwarding `header` events to IPC clients.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableBlockAnnounceIpc","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_setAnnounceFilter` / `node_getAnnounceFilter`

Global filter applied before any `header` frame is sent to IPC clients.

| `announce_type` | `value` | Effect |
|-----------------|---------|--------|
| `"count"` | `0` | All announces per block |
| `"count"` | `N` | First `N` announces per block (`1` = first only) |
| `"delay_time"` | `ms` | Only announces where `delay_time_ms == value` |

Default filter config: `count` / `1` (active only when block announce IPC is enabled).

```bash
# Set: first 10 announces per block
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setAnnounceFilter","params":["count",10],"id":1}'

# Get current filter
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_getAnnounceFilter","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
{"jsonrpc":"2.0","id":1,"result":["count",10]}
```

---

### Mempool IPC & logging

#### `node_enableMempoolWatcher` / `node_disableMempoolWatcher`

Start/stop the pool import notification stream (required before mempool IPC events).

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableMempoolWatcher","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_enableMempoolIpc` / `node_disableMempoolIpc`

Allow mempool events to reach IPC clients (clients must still opt in with `set_require_mempool`).

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_enableMempoolLog` / `node_disableMempoolLog`

Log each pool import + ready-queue order to `bot::pool`.

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

---

### Logging toggles

| Method | Log target | Sample output |
|--------|------------|---------------|
| `node_enableUserLog` | `bot=*` only (hides Substrate logs) | — |
| `node_disableUserLog` | Substrate default (suppresses `bot::*`) | — |
| `node_enablePeerAnnounceTimingLog` | `bot::metrics` | `peer_announce_timing block=8348334 peer=12D3Koo… index=1 delay_time_ms=983` |
| `node_enablePeerRttLog` | `bot::metrics` | `peer_rtt peer=12D3Koo… rtt_ms=42` |
| `node_enableTxInclusionDelayLog` | `bot::metrics` | `tx_inclusion_delay tx=0x… block=8348335 delay_ms=1234` |

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableUserLog","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

---

### Peer management

#### `node_peerConnect`

Dial a peer by multiaddr.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerConnect","params":["/ip4/88.216.36.114/tcp/30333/p2p/12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt"],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":"12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt"}
```

#### `node_peerDisconnect`

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerDisconnect","params":["12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt"],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_peerDisconnectAll`

```json
{"jsonrpc":"2.0","id":1,"result":47}
```

#### `node_peerConnectFromFile`

Load multiaddrs from file (one per line). Adds to custom/reserved set; does not disconnect existing peers.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerConnectFromFile","params":["config/reserved.txt"],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "loaded": 55,
    "peer_ids": ["12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt"],
    "multiaddrs": ["/ip4/88.216.36.114/tcp/30333/p2p/12D3Koo…"]
  }
}
```

#### `node_enableNormalPeer` / `node_disableNormalPeer` / `node_clearNormalPeers`

Normal (non-reserved) sync peers are **disabled by default**.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableNormalPeer","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_disableNormalPeer","params":[],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":{"disconnected":12}}
```

#### `node_enableLogPeer` / `node_disableLogPeer`

Append newly seen peers to a log file; enables `find_peer` IPC for opted-in clients.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_enableLogPeer","params":["peer_log.txt"],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_peerSetMode`

| `mode` | Label | Behaviour |
|--------|-------|-----------|
| `0` | `only_custom` | Dial only custom/reserved peers |
| `1` | `both` | Custom + system targets |
| `2` | `only_system` | System targets only |

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerSetMode","params":[0],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":0}
```

#### `node_peerSetCheckingTime`

Custom peer dial loop timing (milliseconds).

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerSetCheckingTime","params":[5000,10000],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_peerStatus`

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "mode": "only_custom",
    "checking_ms": 5000,
    "sleep_ms": 10000,
    "normal_peers_enabled": false,
    "peer_log_enabled": true,
    "peer_log_path": "peer_log.txt",
    "custom_peer_count": 55,
    "custom_open_stream": 48,
    "normal_connected": 0,
    "custom_connected": 48,
    "system_reserved_count": 0,
    "connected_total": 48,
    "custom_peers": [
      {
        "peer_id": "12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt",
        "multiaddr": "/ip4/88.216.36.114/tcp/30333/p2p/12D3Koo…",
        "connected": true,
        "libp2p": true,
        "sync": true,
        "tx_reserved": true
      }
    ]
  }
}
```

#### `node_peerList`

Full connected-peer snapshot with scores and tracker info.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
      "connected": true,
      "sync": true,
      "libp2p": true,
      "direction": "out",
      "multiaddr": "/ip4/1.2.3.4/tcp/30333/p2p/12D3Koo…",
      "registered_multiaddr": "/ip4/1.2.3.4/tcp/30333/p2p/12D3Koo…",
      "known_addresses": ["/ip4/1.2.3.4/tcp/30333/p2p/12D3Koo…"],
      "role": "FULL",
      "version": "1.0.0",
      "best_hash": "0xabc…",
      "best_number": 8348334,
      "reputation": 0,
      "latest_ping_ms": 42,
      "tx_reserved": true,
      "custom": true,
      "reserved": true,
      "system_target": false,
      "network_reserved": true,
      "disabled": false,
      "peer_log_seen": true,
      "scores": {
        "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
        "connected": true,
        "rtt_ms": 42,
        "avg_rtt_ms": 38,
        "blocks_received_first": 120,
        "first_block_percentage": 0.15,
        "avg_block_announcement_delay_ms": 450,
        "disconnect_count": 2,
        "connect_count": 50,
        "uptime_score": 0.96,
        "rtt_score": 0.85,
        "score": 0.72
      },
      "tracker": {
        "announce_score": 4500,
        "first_announce_hits": 120,
        "tx_propagation_hits": 35,
        "last_best_number": 8348334,
        "roles": "FULL"
      }
    }
  ]
}
```

#### `node_setDisablePeers` / `node_setDisablePeersFromFile`

Replace the disabled peer set, persist to the configured disable-peers file, disconnect and ban matching peers.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setDisablePeers","params":[["12D3KooWBadPeerIdExample123456789012345678901234567890"]],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "applied": 1,
    "disconnected": 1,
    "disabled_peers": ["12D3KooWBadPeerIdExample123456789012345678901234567890"],
    "invalid_peer_ids": []
  }
}
```

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setDisablePeersFromFile","params":["config/disable_peers.txt"],"id":1}'
```

#### `node_peerFindClosest`

DHT lookup: peers closest to a target peer id.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_peerFindClosest","params":["12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5"],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "target": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
    "peers": [
      {
        "peer_id": "12D3KooWDQQaxJ7TspWhBRrdMJzYrQdZVwvte4iJmb6gdYxMzoSt",
        "multiaddrs": ["/ip4/88.216.36.114/tcp/30333/p2p/12D3Koo…"]
      }
    ]
  }
}
```

#### `node_peerScores`

All tracked peers ranked by composite score (descending).

Score formula: `0.6 × first_block_percentage + 0.3 × rtt_score + 0.1 × uptime_score`

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "total_blocks": 800,
    "peers": [
      {
        "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
        "connected": true,
        "rtt_ms": 42,
        "avg_rtt_ms": 38,
        "blocks_received_first": 120,
        "first_block_percentage": 0.15,
        "avg_block_announcement_delay_ms": 450,
        "disconnect_count": 2,
        "connect_count": 50,
        "uptime_score": 0.96,
        "rtt_score": 0.85,
        "score": 0.72
      }
    ]
  }
}
```

---

### Slot state (block announce analytics)

Aggregates announce timing per slot position (`block_number % 20`).

#### `node_slotState`

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "slots": [
      {
        "slot": 14,
        "first_announce_peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
        "peers": [
          {
            "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
            "avg_delay_time_ms": 450,
            "min_delay_time_ms": 120,
            "max_delay_time_ms": 980,
            "announce_count": 85
          }
        ]
      }
    ]
  }
}
```

#### `node_slotStateBySlot`

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_slotStateBySlot","params":[14],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "slot": 14,
    "first_announce_peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
    "peers": [
      {
        "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
        "avg_delay_time_ms": 450,
        "min_delay_time_ms": 120,
        "max_delay_time_ms": 980,
        "announce_count": 85
      }
    ]
  }
}
```

---

### Transaction propagation (global)

Default gossip for pool submits (including IPC) when no per-submit IPC override is set.

#### `node_setPropagateMode` / `node_propagateMode`

| Mode | Value | Label | Behaviour |
|------|-------|-------|-----------|
| Normal | `0` | `normal` | Rank peers (allowlist / reserved-first / max peers) |
| Announce first | `1` | `announce` | Gossip only to last block announcer |
| Parallel | `2` | `parallel` | Gossip to all tx peers in parallel |

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setPropagateMode","params":[0],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":0}
```

#### `node_enableTxPropagationFirstReservedNode` / `node_disableTxPropagationFirstReservedNode`

When enabled, `--reserved-nodes` / custom peers are ordered first in normal-mode ranking.

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_setTxPropagationMaxPeers`

Cap outbound peers per round in normal mode. `0` = no limit.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_setTxPropagationMaxPeers","params":[5],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":true}
```

#### `node_propagateToPeers`

Restrict **all** outbound gossip to an allowlist (base58 peer id or `/p2p/…` multiaddr). Empty array clears allowlist.

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_propagateToPeers","params":[["12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5"]],"id":1}'
```

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "enabled": true,
    "peers": ["12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5"],
    "invalid_peer_ids": []
  }
}
```

Clear allowlist:

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_propagateToPeers","params":[[]],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":{"enabled":false,"peers":[],"invalid_peer_ids":[]}}
```

#### `node_ownPropagationLatest` / `node_ownPropagationHistory`

Diagnostics for bot-initiated propagations. History `limit` defaults to 20, max 100.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "tx_hash": "0x1a2b3c…",
    "last_block_number": 8348334,
    "announced_peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
    "timestamp_ms": 1749510468500,
    "propagate_time_ms": 12,
    "propagate_peers": [
      {
        "peer_id": "12D3KooWEKCdC2F61VwEB9GpdrC9AL6nXHJTs7rUH7SoJoSQJ5A5",
        "addr": "/ip4/1.2.3.4/tcp/30333/p2p/12D3Koo…"
      }
    ]
  }
}
```

```bash
curl -s http://127.0.0.1:9944 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"node_ownPropagationHistory","params":[10],"id":1}'
```

```json
{"jsonrpc":"2.0","id":1,"result":[{ "...OwnPropagationRecord..." }]}
```

When no propagation has occurred yet:

```json
{"jsonrpc":"2.0","id":1,"result":null}
```

---

## End-to-end setup

### 1. Operator — enable features via RPC

```bash
RPC=http://127.0.0.1:9944
HDR='Content-Type: application/json'

# IPC socket (required)
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_startIpc","params":["/tmp/subtensor-bot.sock"],"id":1}'

# Block announces → IPC
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableBlockAnnounceIpc","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_setAnnounceFilter","params":["count",1],"id":1}'

# Optional: mempool, logging, peers, propagation
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableMempoolWatcher","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableMempoolIpc","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableMempoolLog","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableNormalPeer","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableUserLog","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_enableTxPropagationFirstReservedNode","params":[],"id":1}'
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_setTxPropagationMaxPeers","params":[5],"id":1}'

# Verify
curl -s $RPC -H "$HDR" -d '{"jsonrpc":"2.0","method":"node_status","params":[],"id":1}'
```

### 2. Bot — IPC client settings

```rust
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
