# Integrating subtensor-bot on another node server

The bot is a **library crate** (`bot/`). It does not run standalone — it must be wired into `node-subtensor`.

This repo is a **public fork** of [opentensor/subtensor](https://github.com/opentensor/subtensor) with bot integration:

| Repository | URL | Role |
|------------|-----|------|
| **This fork (origin)** | https://github.com/DevMaster4U/subtensor | Subtensor node + bot integration |
| **Upstream** | https://github.com/opentensor/subtensor | OpenTensor canonical source |
| **Bot crate only** | https://github.com/DevMaster4U/subtensor-bot | Standalone `bot/` crate (optional) |

---

## Quick setup (recommended)

Run on your Linux node server:

```bash
# 1. Clone this fork (includes bot integration on bot-test branch, or apply patch on main)
git clone https://github.com/DevMaster4U/subtensor.git
cd subtensor
git remote add upstream https://github.com/opentensor/subtensor.git   # skip if already present

# 2. Wire bot into node (idempotent — safe if patch failed or was partial)
chmod +x bot/scripts/apply-integration.sh
./bot/scripts/apply-integration.sh

# Alternative if you prefer patch (fragile on non-main branches):
# git apply --whitespace=nowarn bot/node-integration.patch

# Alternative: two-repo setup — clone bot crate from subtensor-bot instead of using bot/ here
# git clone https://github.com/DevMaster4U/subtensor-bot.git /tmp/subtensor-bot
# cp -r /tmp/subtensor-bot/bot ./bot

# 3. Configure bot
cp bot/.env.example .env   # or create .env in repo root — see README.md
# Edit .env: BOT_PRIVATE_KEY, BOT_TO, etc.

# 4. Build
cargo build -p node-subtensor --release

# 5. Run node with RPC
./target/release/node-subtensor --chain finney --rpc-port 9933 --rpc-cors all
```

Then control the bot via RPC — see [README.md](./README.md).

---

## What the bot adds beyond `bot/`

These files live in **DevMaster4U/subtensor** (node integration; not in the standalone bot repo):

| File | Purpose |
|------|---------|
| `Cargo.toml` | Add `"bot"` workspace member + `subtensor-bot` dependency |
| `node/Cargo.toml` | Add `subtensor-bot = { workspace = true }` |
| `node/src/lib.rs` | `pub mod bot_block_announce;` |
| `node/src/bot_block_announce.rs` | Pre-validation block announce hook |
| `node/src/service.rs` | Hub, RPC, `start_bot`, `start_pool_injector` |

All of this is bundled in [`node-integration.patch`](./node-integration.patch).

---

## Sync with upstream OpenTensor

```bash
cd subtensor

# Update from OpenTensor upstream
git fetch upstream
git merge upstream/main    # or: git rebase upstream/main

# Push your fork
git push origin main
```

### Using git submodule (optional)

```bash
cd subtensor
git submodule add https://github.com/DevMaster4U/subtensor-bot.git vendor/subtensor-bot

# Point workspace at the nested bot crate — in root Cargo.toml:
#   members = [ ..., "vendor/subtensor-bot/bot" ]
#   subtensor-bot = { path = "vendor/subtensor-bot/bot" }
```

Then apply `node-integration.patch` (adjust paths if you changed the bot location).

---

## Patch failed?

If `git apply bot/node-integration.patch` errors like:

```
error: Cargo.toml: patch does not apply
error: node/src/bot_block_announce.rs: already exists
```

That means **partial integration** or your subtensor branch differs from the patch base. Use the idempotent script instead:

```bash
chmod +x bot/scripts/apply-integration.sh
./bot/scripts/apply-integration.sh
```

Safe to re-run — skips steps already done.

---

## Manual integration

If `git apply bot/node-integration.patch` fails due to upstream changes:

### 1. Root `Cargo.toml`

```toml
[workspace]
members = [
    "bot",          # add this line
    # ...
]

[workspace.dependencies]
subtensor-bot = { path = "bot" }   # add this line
```

### 2. `node/Cargo.toml`

```toml
subtensor-bot = { workspace = true }
```

### 3. `node/src/lib.rs`

```rust
pub mod bot_block_announce;
```

### 4. Copy new file

Copy `bot/node-integration.patch` and extract `node/src/bot_block_announce.rs`, or copy from a machine that already has the integration.

### 5. `node/src/service.rs`

Add these imports at the top:

```rust
use subtensor_bot::rpc::BotApiServer;
```

Before `build_network`, create hub and control:

```rust
let (announce_hub, _) = subtensor_bot::announce::BlockAnnounceHub::new();
let announce_hub_for_network = announce_hub.clone();
let announce_rx = announce_hub.subscribe();
let bot_control = Arc::new(subtensor_bot::control::BotControl::new());
let peer_tracker = Arc::new(subtensor_bot::peers::PeerTracker::new());
```

Pass the validator builder to `build_network`:

```rust
block_announce_validator_builder: Some(Box::new(move |client| {
    Box::new(crate::bot_block_announce::NotifyingBlockAnnounceValidator::new(
        client,
        announce_hub_for_network,
    ))
})),
```

In the RPC builder closure, merge bot RPC:

```rust
let bot_control = bot_control.clone();
let peer_tracker = peer_tracker.clone();
// ... inside the closure, after create_full:
module.merge(subtensor_bot::rpc::BotRpc::new(bot_control, peer_tracker).into_rpc())?;
```

After `spawn_frontier_tasks`, start bot tasks:

```rust
subtensor_bot::processor::start_bot(
    &task_manager,
    client.clone(),
    transaction_pool.clone(),
    sync_service.clone(),
    announce_rx,
    bot_control.clone(),
    peer_tracker,
);
subtensor_bot::pool_inject::start_pool_injector(
    &task_manager,
    client.clone(),
    transaction_pool.clone(),
    bot_control,
);
```

---

## Environment on the server

Create `.env` in the **subtensor repo root** (not inside `bot/`):

```env
BOT_PRIVATE_KEY=your_64_char_hex_key
BOT_TO=0000000000000000000000000000000000000001
BOT_CHAIN_ID=964
```

See [README.md](./README.md) for all variables and RPC usage.

---

## Verify integration

```bash
# Build succeeds
cargo build -p node-subtensor --release

# RPC responds after node start
curl -s -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"bot_status","params":[],"id":1}' \
  http://127.0.0.1:9933
```

Expected: `{ "running": false, "tx_remaining": null, "tx_sent": 0, "inject_mode": "announce" }`

---

## Architecture reminder

```
DevMaster4U/subtensor (fork)     DevMaster4U/subtensor-bot (optional)
├── node/                          └── bot/
├── runtime/                           ├── src/ (bot logic)
├── pallets/                           ├── README.md
└── + patch wires bot ──────────────► node-integration.patch
         ▲
         │ sync
upstream: opentensor/subtensor
```

Develop in this fork. Publish only `bot/` to **subtensor-bot** if you want a standalone bot crate repo.

---

## Publishing updates to the bot repository

Develop in the full **subtensor monorepo** on branch `bot-test`. Publish only `bot/` to **DevMaster4U/subtensor-bot**.

### Local layout

| Branch | Purpose |
|--------|---------|
| `bot-test` | Full monorepo — bot + node integration (local dev) |
| `subtensor-bot-main` | Orphan branch — only `bot/` folder, tracks `origin/main` |

| Remote | URL |
|--------|-----|
| `origin` | https://github.com/DevMaster4U/subtensor.git |
| `upstream` | https://github.com/opentensor/subtensor.git |
| `bot-origin` | https://github.com/DevMaster4U/subtensor-bot.git (add when publishing bot crate only) |

Add the bot publish remote once:

```bash
git remote add bot-origin https://github.com/DevMaster4U/subtensor-bot.git
```

---

### Case A: Bot code only (`bot/src/*`, tests, README)

No patch update needed.

```bash
# 1. Commit on bot-test
git checkout bot-test
git add bot/
git commit -m "your message"

# 2. Copy bot/ to the publish branch and push
git checkout subtensor-bot-main
git checkout bot-test -- bot/
git commit -m "your message"
git push bot-origin subtensor-bot-main:main

# 3. Return to dev branch
git checkout bot-test
```

---

### Case B: Node integration changed (`node/src/service.rs`, `bot_block_announce.rs`, etc.)

Regenerate the patch, then push.

```bash
git checkout bot-test

# 1. Commit all changes (bot + node)
git add bot/ node/src/service.rs node/src/bot_block_announce.rs node/src/lib.rs Cargo.toml node/Cargo.toml
git commit -m "your message"

# 2. Regenerate patch (base = upstream OpenTensor main; adjust if you use another base)
git fetch upstream main
git diff upstream/main..bot-test -- \
  Cargo.toml \
  node/Cargo.toml \
  node/src/lib.rs \
  node/src/service.rs \
  node/src/bot_block_announce.rs \
  > bot/node-integration.patch

# On Linux the patch uses LF automatically.
# On Windows, open the file and save with LF line endings if git apply warns on Linux servers.

# 3. Commit the updated patch
git add bot/node-integration.patch
git commit -m "Update node-integration.patch"

# 4. Push bot/ to GitHub
git checkout subtensor-bot-main
git checkout bot-test -- bot/
git commit -m "your message (include patch update)"
git push bot-origin subtensor-bot-main:main

git checkout bot-test
```

---

### Case C: One-liner push script (Linux / macOS)

From the subtensor repo root, after committing on `bot-test`:

```bash
./bot/scripts/publish-to-github.sh "your commit message"
```

See [`scripts/publish-to-github.sh`](./scripts/publish-to-github.sh).

---

### On your node server after a bot repo update

```bash
cd /path/to/subtensor
git -C /tmp/subtensor-bot pull
cp -r /tmp/subtensor-bot/bot ./bot
git apply --whitespace=nowarn bot/node-integration.patch   # safe to re-run if already applied
cargo build -p node-subtensor --release
```

If the patch was already applied, `git apply` may fail — that is fine; just rebuild.

---

### Tips

- **Never push the full subtensor monorepo** to `subtensor-bot` — it is too large and times out.
- **Always regenerate the patch** when `node/src/service.rs` or related files change.
- Test locally before pushing: `cargo build -p node-subtensor --release`
- Keep `upstream` pointed at OpenTensor so `git diff upstream/main..bot-test` stays accurate.

