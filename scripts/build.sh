#!/usr/bin/env bash
# Prepare a named node instance: binary + config under nodes/<service_name>/.
#
# Usage:
#   ./scripts/build.sh <service_name>              # build + copy (default)
#   ./scripts/build.sh <service_name> build        # build + copy
#   ./scripts/build.sh <service_name> copy         # copy existing production binary only
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  echo "Usage: $0 <service_name> [build|copy]" >&2
  echo "  build  cargo build + install to nodes/<service_name>/ (default)" >&2
  echo "  copy   copy target/production/node-subtensor only (skip cargo build)" >&2
  exit 1
}

[[ $# -ge 1 ]] || usage
SERVICE_NAME="$1"
MODE="${2:-build}"

if [[ ! "$SERVICE_NAME" =~ ^[a-zA-Z][a-zA-Z0-9_-]*$ ]]; then
  echo "error: invalid service name '$SERVICE_NAME' (use letters, digits, -, _)" >&2
  exit 1
fi

if [[ "$MODE" != "build" && "$MODE" != "copy" ]]; then
  usage
fi

# shellcheck source=node_paths.sh
source "$REPO_ROOT/scripts/node_paths.sh"

install_binary() {
  local src="$1" dest="$2"
  install -d -m 0755 "$(dirname "$dest")"
  local src_real dest_real
  src_real="$(readlink -f "$src")"
  dest_real="$(readlink -f "$dest" 2>/dev/null || echo "")"
  if [[ -n "$dest_real" && "$src_real" == "$dest_real" ]]; then
    echo "Binary already at $dest"
    return 0
  fi
  install -m 0755 "$src" "$dest"
}

copy_config_templates() {
  install -d -m 0755 "$CONFIG_DIR"
  install -m 0755 "$TEMPLATE_CONFIG/run_node.sh" "$RUN_SCRIPT"
  if [[ ! -f "$CONFIG_DIR/reserved.txt" ]]; then
    install -m 0644 "$TEMPLATE_CONFIG/reserved.txt" "$CONFIG_DIR/reserved.txt"
  fi
  if [[ ! -f "$CONFIG_DIR/disable_peers.txt" ]]; then
    install -m 0644 "$TEMPLATE_CONFIG/disable_peers.txt" "$CONFIG_DIR/disable_peers.txt"
  fi
}

write_subtensor_env() {
  copy_config_templates
  if [[ -f "$ENV_DEST" ]]; then
    sed -i \
      -e "s|^SUBTENSOR_SERVICE_NAME=.*|SUBTENSOR_SERVICE_NAME=$SERVICE_NAME|" \
      -e "s|^SUBTENSOR_INSTALL_PREFIX=.*|SUBTENSOR_INSTALL_PREFIX=$SERVICE_DIR|" \
      -e "s|^SUBTENSOR_BINARY=.*|SUBTENSOR_BINARY=$BIN_DEST|" \
      -e "s|^SUBTENSOR_CONFIG_DIR=.*|SUBTENSOR_CONFIG_DIR=$CONFIG_DIR|" \
      -e "s|^SUBTENSOR_BASE_PATH=.*|SUBTENSOR_BASE_PATH=$BASE_PATH|" \
      "$ENV_DEST"
    echo "Updated paths in $ENV_DEST"
    return
  fi

  sed \
    -e "s|^SUBTENSOR_SERVICE_NAME=.*|SUBTENSOR_SERVICE_NAME=$SERVICE_NAME|" \
    -e "s|^SUBTENSOR_INSTALL_PREFIX=.*|SUBTENSOR_INSTALL_PREFIX=$SERVICE_DIR|" \
    -e "s|^SUBTENSOR_BINARY=.*|SUBTENSOR_BINARY=$BIN_DEST|" \
    -e "s|^SUBTENSOR_CONFIG_DIR=.*|SUBTENSOR_CONFIG_DIR=$CONFIG_DIR|" \
    -e "s|^SUBTENSOR_BASE_PATH=.*|SUBTENSOR_BASE_PATH=$BASE_PATH|" \
    "$TEMPLATE_ENV" > "$ENV_DEST"
  echo "Created $ENV_DEST"
}

if [[ "$MODE" == "build" ]]; then
  echo "Building node-subtensor (profile=production, features=metadata-hash) ..."
  cargo build -p node-subtensor --profile=production --features=metadata-hash
else
  echo "Copy-only mode (skipping cargo build) ..."
fi

if [[ ! -f "$BUILD_BINARY" ]]; then
  echo "error: production binary not found at $BUILD_BINARY" >&2
  echo "Run: $0 $SERVICE_NAME build" >&2
  exit 1
fi

install_binary "$BUILD_BINARY" "$BIN_DEST"
write_subtensor_env

echo ""
echo "Node instance ready: $SERVICE_NAME"
echo "  binary:  $BIN_DEST"
echo "  config:  $CONFIG_DIR"
echo "  data:    $BASE_PATH (created on first run / install)"
echo ""
echo "Install systemd:  sudo ./scripts/install_service.sh $SERVICE_NAME [--start]"
echo "Run directly:     SUBTENSOR_ENV_FILE=$ENV_DEST $RUN_SCRIPT"
