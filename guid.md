# Subtensor Authority Discovery Reference

Portable guide for finding block-authority information from **your own running Subtensor node**.
No local `polkadot-sdk` checkout required — all source links point to GitHub.

---

## Table of contents

1. [Architecture overview](#1-architecture-overview)
2. [On-node file paths](#2-on-node-file-paths)
3. [RPC quick reference](#3-rpc-quick-reference)
4. [Step-by-step: consensus authorities (on-chain keys)](#4-step-by-step-consensus-authorities-on-chain-keys)
5. [Step-by-step: predict next block author (Aura)](#5-step-by-step-predict-next-block-author-aura)
6. [Step-by-step: network addresses (PeerId / IP / port)](#6-step-by-step-network-addresses-peerid--ip--port)
7. [Step-by-step: connect directly to a peer](#7-step-by-step-connect-directly-to-a-peer)
8. [Transaction pool ordering (fast inclusion)](#8-transaction-pool-ordering-fast-inclusion)
9. [Subtensor-specific: MEV Shield pallet](#9-subtensor-specific-mev-shield-pallet)
10. [Source code index (GitHub links)](#10-source-code-index-github-links)

---

## 1. Architecture overview

Substrate chains split authority information into **two separate layers**:

| Layer | What you get | Where it lives |
|-------|--------------|----------------|
| **Consensus keys** | sr25519/ed25519 public keys, slot rotation | On-chain storage + runtime APIs |
| **Network addresses** | libp2p multiaddr `/ip4/…/tcp/…/p2p/PeerId` | Off-chain P2P layer only |

**Critical for Subtensor:** the `pallet-authority-discovery` was **removed** from the Subtensor runtime
([PR #1708](https://github.com/opentensor/subtensor/pull/1708)). There is **no DHT lookup**
(`AuthorityDiscoveryApi`) on Finney/mainline Subtensor today. You cannot rely on
`authority_discovery_addr_cache.json`.

Subtensor currently uses **Aura** block authoring (+ GRANDPA finality), with a hybrid Aura/Babe node
in development ([PR #1876](https://github.com/opentensor/subtensor/pull/1876)).

**Subtensor runtime session keys** (no `audi` / authority-discovery key):

- Source: [opentensor/subtensor `runtime/src/lib.rs` — `SessionKeys`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)

```rust
pub struct SessionKeys {
    pub aura: Aura,
    pub grandpa: Grandpa,
}
```

---

## 2. On-node file paths

Assume your node was started with `--base-path /var/lib/subtensor` (adjust to your setup).

| Path | Contents |
|------|----------|
| `{base-path}/chains/{chain-id}/` | Chain database, keystore, network state |
| `{base-path}/chains/{chain-id}/network/` | P2P state |
| `{base-path}/chains/{chain-id}/network/secret_ed25519` | Node key (keep private) |
| `{base-path}/chains/{chain-id}/network/peer_id` | This node's libp2p PeerId |
| `{base-path}/chains/{chain-id}/keystore/` | Validator keys (only if you run a validator) |
| `{base-path}/chains/{chain-id}/db/` | Block/state database |

**Typical defaults**

| Setting | Default / common value |
|---------|------------------------|
| RPC HTTP | `http://127.0.0.1:9933` |
| RPC WebSocket | `ws://127.0.0.1:9944` |
| P2P port | `30333` |
| `--base-path` | `/var/lib/subtensor` (Linux prod) or path you passed on CLI |
| `{chain-id}` | From `--chain` flag (e.g. Finney chainspec name) |

**Find your actual paths from logs** — on startup the node prints base path and chain:

```bash
# Example node command (from Subtensor docs)
./target/production/node-subtensor \
  --chain ./chainspecs/raw_spec_finney.json \
  --base-path /var/lib/subtensor \
  --rpc-port 9944 \
  --rpc-external \
  --port 30333
```

Docs: [Run a Subtensor Node](https://subtensor.com/learn/guides/node-operations)

**What is NOT on Subtensor nodes**

| File | Status |
|------|--------|
| `network/authority_discovery_addr_cache.json` | Only exists if `pallet-authority-discovery` is enabled — **not on Subtensor** |

Polkadot-SDK reference for that cache (other chains only):
[sc_authority_discovery `ADDR_CACHE_FILE_NAME`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/authority-discovery/src/worker.rs#L76)

---

## 3. RPC quick reference

Set your RPC endpoint once:

```bash
export RPC=http://127.0.0.1:9933
# or WebSocket: ws://127.0.0.1:9944
```

Helper function (bash):

```bash
rpc() {
  curl -s -H "Content-Type: application/json" \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}" \
    "$RPC" | jq .
}
```

| RPC method | Purpose |
|------------|---------|
| `system_localPeerId` | Your node's PeerId |
| `system_localListenAddresses` | Your node's multiaddrs |
| `system_peers` | Connected peers (PeerId + observed addresses) |
| `system_addReservedPeer` | Force persistent connection to a multiaddr |
| `system_reservedPeers` | List reserved peers |
| `system_nodeRoles` | `Authority` or `Full` |
| `chain_getHeader` | Block header + digest (Aura slot / author index) |
| `chain_getBlock` | Full block |
| `state_call` | Call runtime APIs (best way to get authorities) |
| `state_getStorage` | Read raw pallet storage |
| `state_getMetadata` | Full runtime metadata (for storage key tooling) |
| `author_submitExtrinsic` | Submit transaction |
| `author_pendingExtrinsics` | Current ready pool on **your** node |

RPC trait definitions (Polkadot-SDK source):

- [System API](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/system/mod.rs)
- [Author API](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/author/mod.rs)
- [Chain API](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/chain/mod.rs)

---

## 4. Step-by-step: consensus authorities (on-chain keys)

### Method A — `state_call` (recommended, no metadata tooling needed)

#### Aura authorities (block producers)

```bash
rpc state_call '["AuraApi_authorities","0x",null]'
```

Returns SCALE-encoded `Vec<AuraId>` (sr25519 public keys). On Subtensor these map directly to
`AccountId32` (see
[`BlockAuthorFromAura`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)).

#### Aura slot duration (milliseconds)

```bash
rpc state_call '["AuraApi_slot_duration","0x",null]'
```

Used to compute current slot from block timestamp.

#### GRANDPA authorities (finality, not block production)

```bash
rpc state_call '["GrandpaApi_grandpa_authorities","0x",null]'
rpc state_call '["GrandpaApi_current_set_id","0x",null]'
```

Runtime API trait (Polkadot-SDK):

- [AuraApi](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/primitives/consensus/aura/src/lib.rs)
- [GrandpaApi](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/primitives/consensus/grandpa/src/lib.rs)

On-chain storage (Subtensor Aura config):

- [Subtensor `pallet_aura::Config`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs) — `MaxAuthorities = 32`
- [Polkadot-SDK `pallet_aura::Authorities`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/aura/src/lib.rs)

### Method B — Polkadot.js (easiest to decode)

Connect to `ws://127.0.0.1:9944`, then in browser console or script:

```javascript
const api = await ApiPromise.create({ provider: new WsProvider('ws://127.0.0.1:9944') });
const authorities = await api.query.aura.authorities();
const grandpa = await api.query.grandpa.authorities();
console.log('Aura authorities:', authorities.toHuman());
```

### Method C — `state_getStorage` (raw hex)

Get metadata first, then derive keys with `@polkadot/api` or `subkey inspect` on metadata.
Prefer Method A or B unless you are building low-level tooling.

---

## 5. Step-by-step: predict next block author (Aura)

Aura rotates deterministically:

> For slot `s`, author index = `s % |authorities|`

Source:

- [Aura README](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/consensus/aura/README.md)
- [`slot_author()` in `standalone.rs`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/consensus/aura/src/standalone.rs#L70-L86)

Subtensor uses the same formula in
[`FindAuraAuthors`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs).

### Step 1 — Get authority list

Use `AuraApi_authorities` (section 4).

### Step 2 — Get current slot from latest block header

```bash
rpc chain_getHeader '[]'
```

In the response, find the digest log with engine ID **`aura`** (`[97, 117, 114, 97]`).
The payload is the SCALE-encoded slot number (`u64`).

Aura engine ID constant:
[`AURA_ENGINE_ID`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/primitives/consensus/aura/src/lib.rs#L69-L70)

**Easier:** use Polkadot.js:

```javascript
const header = await api.rpc.chain.getHeader();
const slot = api.createType('Slot', header.digest.logs[0].asPreRuntime[1]);
console.log('Current slot:', slot.toNumber());
```

### Step 3 — Compute upcoming authors

```javascript
const authorities = await api.query.aura.authorities();
const header = await api.rpc.chain.getHeader();
const slot = api.createType('Slot', header.digest.logs[0].asPreRuntime[1]).toNumber();

for (let i = 0; i < 5; i++) {
  const nextSlot = slot + i;
  const idx = nextSlot % authorities.length;
  console.log(`Slot ${nextSlot} → authority[${idx}]`, authorities[idx].toHuman());
}
```

### Step 4 — Map authority key → account (Subtensor)

Subtensor resolves block author as:

```rust
AccountId32::new(authority_id.to_raw_vec().try_into().ok()?)
```

So the Aura sr25519 public key **is** the block-author account on Subtensor.

### Who can change the authority set?

Subtensor exposes `AdminUtils` → `change_authorities` on the Aura pallet interface:

- [`AuraPalletIntrf::change_authorities`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)

This is **root-governed**, not permissionless.

---

## 6. Step-by-step: network addresses (PeerId / IP / port)

Because Subtensor **does not** run authority discovery, there is **no on-chain or RPC method**
that returns validator IP addresses. Use these on-node sources instead.

### Source 1 — `system_peers` (peers your node is connected to)

```bash
rpc system_peers '[]'
```

Returns for each peer:

- `peerId` — libp2p PeerId (base58)
- `bestHash`, `bestNumber` — their chain head
- `roles` — `AUTHORITY`, `FULL`, or `LIGHT`
- `protocolVersion`, `genesisHash`

**Important:** `roles: AUTHORITY` tells you the peer **claims** to be a validator node, but
does **not** map PeerId → Aura public key automatically.

Implementation:
[`PeerInfo` / system peers RPC](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/system/helpers.rs)

### Source 2 — Bootnodes from your chainspec

Bootnodes are in the JSON chainspec passed to `--chain`. Example field:

```json
"bootNodes": [
  "/dns/bootnode.finney.chain.opentensor.ai/tcp/30333/ws/p2p/12D3KooW..."
]
```

These are **network entry points**, not necessarily current block authors.

### Source 3 — Your node's own addresses (for comparison)

```bash
rpc system_localPeerId '[]'
rpc system_localListenAddresses '[]'
```

### Source 4 — Block author correlation (heuristic)

There is no standard RPC to ask “which PeerId belongs to Aura key X”. Practical heuristics:

1. Predict next Aura author (section 5).
2. Watch `system_peers` around block production windows.
3. Track peers whose `bestNumber` advances first when new blocks appear.
4. Maintain an off-chain mapping `{ aura_pubkey → multiaddr }` you learn over time.

Polkadot's validator address resolution via DHT ( **not available on Subtensor** ):

- [Authority discovery worker](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/authority-discovery/src/worker.rs)
- [AddrCache](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/authority-discovery/src/worker/addr_cache.rs)

### Source 5 — External validator operator endpoints

If you know a validator's public RPC or P2P endpoint (community lists, your infra, etc.),
verify it independently, then use `system_addReservedPeer` (section 7).

---

## 7. Step-by-step: connect directly to a peer

Reserved peers get **persistent libp2p connections** — useful for faster transaction gossip.

```bash
# multiaddr MUST include /p2p/<PeerId>
rpc system_addReservedPeer '["/ip4/203.0.113.10/tcp/30333/p2p/12D3KooWExamplePeerId"]'

# list reserved peers
rpc system_reservedPeers '[]'

# remove (PeerId only, not full multiaddr)
rpc system_removeReservedPeer '["12D3KooWExamplePeerId"]'
```

RPC definition:
[`system_addReservedPeer`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/system/mod.rs#L84-L99)

Network implementation:
[`set_reserved_peers`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/network/src/service/traits.rs)

**Strategy for MEV / low-latency submission**

1. Compute next 1–3 Aura authors (section 5).
2. If you know their multiaddrs, `system_addReservedPeer` each one.
3. Submit transactions via **WebSocket RPC directly to the block author's node** if they expose RPC.
4. Keep your own node's pool synced as a fallback path.

---

## 8. Transaction pool ordering (fast inclusion)

### There is no “front of pool” injection API

All submission paths (`author_submitExtrinsic`, P2P gossip, local) enter the same pool and are
ordered by **runtime priority**, not arrival time.

Source (explicit comment in RPC):

- [`TX_SOURCE = External` — no special author treatment](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc/src/author/mod.rs#L108-L113)

### What actually controls inclusion order

Priority sort in ready queue:

1. Dependency tags (`requires` / `provides`, e.g. nonce)
2. **`priority` (u64)** — higher first
3. Shorter remaining mortality
4. Older pool insertion (tiebreaker only)

Source:
[`ReadyTransactions::get()` ordering rules](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/transaction-pool/src/graph/ready.rs#L146-L159)

Priority comparison implementation:
[`TransactionRef Ord`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/transaction-pool/src/graph/ready.rs#L55-L62)

### Tip → priority (standard FRAME logic)

Subtensor uses a custom fee handler but still builds on `pallet-transaction-payment` patterns:

- [Subtensor `transaction_payment_wrapper.rs`](https://github.com/opentensor/subtensor/blob/main/runtime/src/transaction_payment_wrapper.rs)
- [Polkadot-SDK `ChargeTransactionPayment::get_priority`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/transaction-payment/src/lib.rs#L871-L931)

**Same-nonce replacement:** new tx must have **strictly higher** combined priority or pool returns
`TooLowPriority`.

Source:
[`replace_previous()` in ready.rs](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/transaction-pool/src/graph/ready.rs)

### Block builder pulls from pool by priority

When the Aura author builds a block:

- [`Proposer::apply_extrinsics`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/basic-authorship/src/basic_authorship.rs)
- Uses `BestIterator` from the pool — not FIFO

### Network propagation timing

Transactions gossip to **all connected full peers** (not only the next author):

- [Propagation loop](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/network/transactions/src/lib.rs#L455-L510)
- Periodic batch interval ≈ **2.9 s**:
  [`PROPAGATE_TIMEOUT`](https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/network/transactions/src/config.rs#L28)

New pool imports also trigger immediate propagation, but you still benefit from being **directly
connected** to the next author.

### Monitor your local pool

```bash
rpc author_pendingExtrinsics '[]'
```

Returns ready transactions on **your node only** — not the block author's pool.

### Practical checklist for fastest inclusion

| Action | Effect |
|--------|--------|
| Submit to **block author's RPC** directly | Skips multi-hop gossip latency |
| `system_addReservedPeer` to upcoming authors | Faster P2P path to their pool |
| **Increase tip** | Raises runtime `priority` — main ordering lever |
| WebSocket RPC + pre-signed extrinsic | Minimizes submit latency |
| Run node near validators (same region) | Lower network RTT |

---

## 9. Subtensor-specific: MEV Shield pallet

Subtensor includes a **`MevShield`** / `pallet_shield` system with slot-based timing windows.
This affects MEV strategy on Subtensor specifically.

Runtime config (milliseconds within each slot):

| Constant | Value | Meaning |
|----------|-------|---------|
| `ShieldAnnounceAtMs` | 7000 | Next ephemeral key announced at 7s into slot |
| `ShieldGraceMs` | 2000 | Previous key still valid for 2s grace |
| `ShieldDecryptWindowMs` | 3000 | Last 3s reserved for decrypt + execute |

Source:
[Subtensor `pallet_shield::Config` parameters](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)

Shield runtime API:

```bash
# Available via metadata — methods include try_decode_shielded_tx, is_shielded_using_current_key
rpc state_getMetadata '[]'
```

Shield API trait in runtime:
[`stp_shield::ShieldApi` impl](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)

Aura author lookup for shield (slot + 2 for next-next author):

- [`FindAuraAuthors`](https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs)

**Implication:** standard plain extrinsics and shielded extrinsics follow different paths.
Study the shield pallet before assuming vanilla Substrate MEV tactics apply unchanged.

---

## 10. Source code index (GitHub links)

### Subtensor (chain-specific)

| Topic | Link |
|-------|------|
| Runtime (Aura, Shield, SessionKeys) | https://github.com/opentensor/subtensor/blob/main/runtime/src/lib.rs |
| Node service (consensus startup) | https://github.com/opentensor/subtensor/blob/main/node/src/service.rs |
| Transaction fee / priority wrapper | https://github.com/opentensor/subtensor/blob/main/runtime/src/transaction_payment_wrapper.rs |
| Hybrid Aura/Babe node PR | https://github.com/opentensor/subtensor/pull/1876 |
| Babe NPoS migration (removed authority discovery) | https://github.com/opentensor/subtensor/pull/1708 |
| Node operations docs | https://subtensor.com/learn/guides/node-operations |

### Polkadot-SDK / Substrate (generic mechanisms)

| Topic | Link |
|-------|------|
| Aura slot author math | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/consensus/aura/src/standalone.rs |
| Aura README | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/consensus/aura/README.md |
| Aura pallet storage (`Authorities`) | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/aura/src/lib.rs |
| GRANDPA pallet | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/grandpa/src/lib.rs |
| Session pallet | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/session/src/lib.rs |
| Block authorship (digest → author) | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/authorship/src/lib.rs |
| Authority discovery client (other chains) | https://github.com/paritytech/polkadot-sdk/tree/master/substrate/client/authority-discovery |
| Authority discovery pallet | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/authority-discovery/src/lib.rs |
| Transaction pool ready queue | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/transaction-pool/src/graph/ready.rs |
| Transaction pool validation | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/transaction-pool/src/common/api.rs |
| Author RPC (submit extrinsic) | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc/src/author/mod.rs |
| System RPC (peers, reserved peers) | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/rpc-api/src/system/mod.rs |
| Transaction gossip / propagation | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/network/transactions/src/lib.rs |
| Block proposer (pool → block) | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/client/basic-authorship/src/basic_authorship.rs |
| Tip → priority formula | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/transaction-payment/src/lib.rs |
| `ValidTransaction` / `propagate` flag | https://github.com/paritytech/polkadot-sdk/blob/master/substrate/primitives/runtime/src/transaction_validity.rs |
| Polkadot validator reserved-peer wiring (reference) | https://github.com/paritytech/polkadot-sdk/blob/master/polkadot/node/network/bridge/src/validator_discovery.rs |

---

## Quick decision tree

```
Need block producer PUBLIC KEY?
  └─ state_call AuraApi_authorities
     or api.query.aura.authorities()

Need NEXT block producer?
  └─ current slot from header digest (engine "aura")
     → authorities[slot % len]

Need IP / PORT / PeerId?
  └─ Subtensor: NO authority discovery
     → system_peers (watch AUTHORITY role peers)
     → bootnodes in chainspec
     → manual / learned mapping
     → system_addReservedPeer when you know multiaddr

Need fastest tx inclusion?
  └─ submit to author's RPC + high tip
     (no front-of-pool API exists)
```

---

*Generated from polkadot-sdk analysis + opentensor/subtensor `main` branch (May 2026).*
*Verify runtime version on your node with `rpc state_getRuntimeVersion '[]'` — APIs may change after upgrades.*
