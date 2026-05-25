# Bot Changelog

History of the `subtensor-bot` crate and its node integration. Each entry references the git commit that introduced the change.

Commits are listed oldest → newest.

---

## [4976a29](https://github.com/opentensor/subtensor/commit/4976a29d58e55e0b55857d1020098967a8b6cd65) — add frontpool logic

**Date:** 2026-05-24  
**Full hash:** `4976a29d58e55e0b55857d1020098967a8b6cd65`

### Added
- **Pool front injection mode** (`bot/src/pool_inject.rs`) — pre-submits transactions to the ready queue immediately on arm and re-injects after on-chain inclusion, targeting FCFS front position within Subtensor's flat priority tier.
- **`InjectMode`** enum in `control.rs` — `OnAnnounce` vs `PoolFront`.
- **`bot_startTxsFront`** RPC method.
- **`inject_mode`** field in `bot_status` response (`"announce"` or `"pool_front"`).
- `start_pool_injector` wired in `node/src/service.rs`.

### Changed
- `processor.rs` skips announce-mode sends when `inject_mode == PoolFront` (peer tracking still runs).
- `announce.rs` docs updated to describe both signal paths.

---

## [f584013](https://github.com/opentensor/subtensor/commit/f5840137c543c6923e0e1f3f322cd2b3c070d375) — add tracking peers

**Date:** 2026-05-24  
**Full hash:** `f5840137c543c6923e0e1f3f322cd2b3c070d375`

### Added
- **`peers.rs`** — correlates first block announce per height with connected peers whose `best_number >= block`.
- Scoring: +1 base per hit, +5 when `best_number == block`, tracks `first_announce_hits`.
- **`bot_peerStats`** RPC — peer leaderboard (default limit 20, max 200).
- **`bot_peerRecommendations`** RPC — top peers for `--reserved-peers` research (default limit 10, max 100).
- Peer attribution logged at `bot::peers` on each first announce per block height.
- `sc-network-sync` dependency for `SyncingService::peers_info()`.

### Changed
- `processor.rs` records peer candidates on first announce per block number.
- `service.rs` creates and passes `PeerTracker` to bot tasks and RPC.

---

## [05b90a3](https://github.com/opentensor/subtensor/commit/05b90a3330e23be30fd073a9b63a9241be5d40c0) — add pre-validation announce

**Date:** 2026-05-24  
**Full hash:** `05b90a3330e23be30fd073a9b63a9241be5d40c0`

### Changed
- **`NotifyingBlockAnnounceValidator`** — moved `hub.notify()` to the synchronous start of `validate()`, before async validation runs. This is the earliest public hook on a non-validator node.
- `announce.rs` docs updated with signal timeline (announce validate → validation complete → import).

---

## [5f1f24a](https://github.com/opentensor/subtensor/commit/5f1f24a5b8bfb323b5a66208a3f95203bfeb76be) — add block announce and rpc bot control

**Date:** 2026-05-24  
**Full hash:** `5f1f24a5b8bfb323b5a66208a3f95203bfeb76be`

### Added
- **`NotifyingBlockAnnounceValidator`** (`node/src/bot_block_announce.rs`) — wraps `DefaultBlockAnnounceValidator` and forwards block announces to the bot via `BlockAnnounceHub`.
- **`announce.rs`** — `BlockAnnounceNotification`, `BlockAnnounceHub` broadcast channel, `is_ahead_of_best` filter.
- **`control.rs`** — `BotControl` shared state: running flag, send budget, nonce resync flag.
- **`rpc.rs`** — JSON-RPC interface:
  - `bot_start`
  - `bot_stop`
  - `bot_startTxs`
  - `bot_status`
- Block-number-based dedup in `processor.rs` (replaces hash-based dedup to handle fork races).

### Changed
- `processor.rs` listens to `announce_rx` instead of `client.import_notification_stream()`.
- `service.rs` creates `BlockAnnounceHub`, registers validator builder, merges `BotRpc` into RPC module.

### Removed
- Burst / initial-wait block logic from processor.

---

## [cf962bc](https://github.com/opentensor/subtensor/commit/cf962bcb6e25f911daec62ba6fcab1afae3524c8) — fix send function

**Date:** 2026-05-24  
**Full hash:** `cf962bcb6e25f911daec62ba6fcab1afae3524c8`

### Changed
- **`PrebuiltTx`** now stores a pre-built `OpaqueExtrinsic` instead of raw RLP bytes.
- `prebuild()` converts `EthTx` → extrinsic at build time (eliminates encode/decode round-trip on the hot path).
- `send()` reduced to `pool.submit_one(best_hash, TransactionSource::Local, tx.extrinsic)`.
- Tests updated in `bot/tests/transact.rs` and `bot/tests/processor.rs`.

---

## [4f277d1](https://github.com/opentensor/subtensor/commit/4f277d1cbd8ddcc64521ca32c748f24c242eb208) — init send transaction after get block header

**Date:** 2026-05-24  
**Full hash:** `4f277d1cbd8ddcc64521ca32c748f24c242eb208`

### Added
- Initial **`subtensor-bot`** crate (`bot/`).
- **`transact.rs`** — EIP-1559 tx building, env-based config, in-process pool submission via `submit_one`.
- **`processor.rs`** — background task triggered on block header / import notifications.
- **`mempool.rs`** — optional ready-pool import watcher using `pool.import_notification_stream()`.
- **`start_bot`** wired into `node/src/service.rs`.
- Environment config: `BOT_PRIVATE_KEY`, `BOT_TO`, `BOT_CHAIN_ID`, `BOT_GAS_LIMIT`, `BOT_MAX_FEE`, `BOT_PRIORITY_FEE`.
- Smoke tests in `bot/tests/`.

---

## Commit index

| Short | Full hash | Summary |
|-------|-----------|---------|
| `4f277d1` | `4f277d1cbd8ddcc64521ca32c748f24c242eb208` | Initial bot crate + in-process submission |
| `cf962bc` | `cf962bcb6e25f911daec62ba6fcab1afae3524c8` | Pre-built extrinsic hot path |
| `5f1f24a` | `5f1f24a5b8bfb323b5a66208a3f95203bfeb76be` | Block announce + RPC control |
| `05b90a3` | `05b90a3330e23be30fd073a9b63a9241be5d40c0` | Pre-validation announce hook |
| `f584013` | `f5840137c543c6923e0e1f3f322cd2b3c070d375` | Peer tracking + stats RPC |
| `4976a29` | `4976a29d58e55e0b55857d1020098967a8b6cd65` | Pool front injection mode |

View full history:

```bash
git log --oneline 4f277d1cb^..4976a29d5 -- bot/ node/src/bot_block_announce.rs node/src/service.rs
```
