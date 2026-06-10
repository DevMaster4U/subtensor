# Node configuration

Shared templates live here. Each node instance gets its own copy under `nodes/<service_name>/config/`.

| Template | Purpose |
|----------|---------|
| `subtensor.env.example` | Per-instance env template (paths filled by `build.sh`) |
| `reserved.txt` | Default reserved peer multiaddrs |
| `disable_peers.txt` | Default blocked peer ids |
| `run_node.sh` | Starts the node using `subtensor.env` |

## Multi-node workflow

```bash
# 1. Build + prepare instance (binary + config)
./scripts/build.sh subtensor1              # cargo build + copy
./scripts/build.sh subtensor2 copy           # copy existing binary only

# 2. Edit per-node settings (ports must differ)
nano nodes/subtensor1/config/subtensor.env
nano nodes/subtensor2/config/subtensor.env

# 3. Install systemd + optional start
sudo ./scripts/install_service.sh subtensor1 --start
sudo ./scripts/install_service.sh subtensor2 --start
```

## Instance layout

```
nodes/
  subtensor1/
    subtensor              # production binary
    config/
      subtensor.env        # SUBTENSOR_BASE_PATH=/var/lib/subtensor1
      reserved.txt
      disable_peers.txt
      run_node.sh
  subtensor2/
    ...
```

Chain data: `/var/lib/<service_name>`

## Environment variables

| Variable | CLI flag | Description |
|----------|----------|-------------|
| `SUBTENSOR_SERVICE_NAME` | — | Instance / systemd unit name |
| `SUBTENSOR_BASE_PATH` | `--base-path` | Chain data (`/var/lib/<service_name>`) |
| `SUBTENSOR_CHAIN` | `--chain` | Chain id or spec path |
| `SUBTENSOR_PORT` | `--port` | P2P port (unique per node) |
| `SUBTENSOR_RPC_PORT` | `--rpc-port` | JSON-RPC port (unique per node) |
| `SUBTENSOR_IN_PEERS` | `--in-peers` | Max inbound peers |
| `SUBTENSOR_OUT_PEERS` | `--out-peers` | Max outbound peers |
| `SUBTENSOR_BOOTNODES` | `--bootnodes` | Comma-separated bootnodes |
| `SUBTENSOR_CONFIG_DIR` | — | Instance config directory |

Override nodes root: `SUBTENSOR_NODES_ROOT=/opt/subtensor/nodes`
