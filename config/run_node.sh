#!/usr/bin/env bash
# Start node-subtensor using settings from config/subtensor.env
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SUBTENSOR_ENV_FILE:-$SCRIPT_DIR/subtensor.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${SUBTENSOR_BINARY:?Set SUBTENSOR_BINARY in $ENV_FILE}"
: "${SUBTENSOR_BASE_PATH:?Set SUBTENSOR_BASE_PATH in $ENV_FILE}"
: "${SUBTENSOR_CHAIN:=finney}"

SUBTENSOR_CONFIG_DIR="${SUBTENSOR_CONFIG_DIR:-$SCRIPT_DIR}"
export SUBTENSOR_CONFIG_DIR
export SUBTENSOR_RESERVED_FILE="${SUBTENSOR_RESERVED_FILE:-$SUBTENSOR_CONFIG_DIR/reserved.txt}"
export SUBTENSOR_DISABLE_PEERS_FILE="${SUBTENSOR_DISABLE_PEERS_FILE:-$SUBTENSOR_CONFIG_DIR/disable_peers.txt}"

if [[ ! -x "$SUBTENSOR_BINARY" ]]; then
  echo "error: binary not found or not executable: $SUBTENSOR_BINARY" >&2
  exit 1
fi

mkdir -p "$SUBTENSOR_BASE_PATH"

ARGS=(
  --base-path "$SUBTENSOR_BASE_PATH"
  --chain "$SUBTENSOR_CHAIN"
  --port "${SUBTENSOR_PORT:-30333}"
  --rpc-port "${SUBTENSOR_RPC_PORT:-9944}"
)

if [[ "${SUBTENSOR_RPC_EXTERNAL:-true}" == "true" ]]; then
  ARGS+=(--rpc-external)
fi

if [[ -n "${SUBTENSOR_RPC_CORS:-all}" ]]; then
  ARGS+=(--rpc-cors "${SUBTENSOR_RPC_CORS}")
fi

if [[ -n "${SUBTENSOR_RPC_MAX_CONNECTIONS:-}" ]]; then
  ARGS+=(--rpc-max-connections "${SUBTENSOR_RPC_MAX_CONNECTIONS}")
fi

if [[ -n "${SUBTENSOR_IN_PEERS:-}" ]]; then
  ARGS+=(--in-peers "${SUBTENSOR_IN_PEERS}")
fi

if [[ -n "${SUBTENSOR_OUT_PEERS:-}" ]]; then
  ARGS+=(--out-peers "${SUBTENSOR_OUT_PEERS}")
fi

if [[ -n "${SUBTENSOR_SYNC:-}" ]]; then
  ARGS+=(--sync "${SUBTENSOR_SYNC}")
fi

if [[ "${SUBTENSOR_NO_MDNS:-false}" == "true" ]]; then
  ARGS+=(--no-mdns)
fi

if [[ -n "${SUBTENSOR_BOOTNODES:-}" ]]; then
  IFS=',' read -ra BOOTNODES <<< "$SUBTENSOR_BOOTNODES"
  for bootnode in "${BOOTNODES[@]}"; do
    bootnode="${bootnode#"${bootnode%%[![:space:]]*}"}"
    bootnode="${bootnode%"${bootnode##*[![:space:]]}"}"
    [[ -n "$bootnode" ]] && ARGS+=(--bootnodes "$bootnode")
  done
fi

if [[ -n "${SUBTENSOR_RESERVED_NODES:-}" ]]; then
  IFS=',' read -ra RESERVED <<< "$SUBTENSOR_RESERVED_NODES"
  for node in "${RESERVED[@]}"; do
    node="${node#"${node%%[![:space:]]*}"}"
    node="${node%"${node##*[![:space:]]}"}"
    [[ -n "$node" ]] && ARGS+=(--reserved-nodes "$node")
  done
fi

if [[ -n "${SUBTENSOR_LOG:-}" ]]; then
  ARGS+=(--log "${SUBTENSOR_LOG}")
fi

if [[ "${SUBTENSOR_ENABLE_LOG_RELOADING:-true}" == "true" ]]; then
  ARGS+=(--enable-log-reloading)
fi

if [[ -n "${SUBTENSOR_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA=( ${SUBTENSOR_EXTRA_ARGS} )
  ARGS+=("${EXTRA[@]}")
fi

exec "$SUBTENSOR_BINARY" "${ARGS[@]}"
