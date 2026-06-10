#!/usr/bin/env bash
# Install a named node instance as a systemd service.
#
# Usage:
#   sudo ./scripts/install_service.sh <service_name> [--start]
#
# Prepare the instance first:
#   ./scripts/build.sh <service_name> [build|copy]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  echo "Usage: sudo $0 <service_name> [--start]" >&2
  echo "  Prepare instance first: ./scripts/build.sh <service_name>" >&2
  exit 1
}

[[ $# -ge 1 ]] || usage
SERVICE_NAME="$1"
START_AFTER=false
shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start) START_AFTER=true ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
  shift
done

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo $0 $SERVICE_NAME" >&2
  exit 1
fi

# shellcheck source=node_paths.sh
source "$REPO_ROOT/scripts/node_paths.sh"

SERVICE_USER="${SUBTENSOR_SERVICE_USER:-subtensor}"
SERVICE_GROUP="${SUBTENSOR_SERVICE_GROUP:-subtensor}"

if [[ ! -f "$BIN_DEST" ]]; then
  echo "error: node binary not found at $BIN_DEST" >&2
  echo "Run first: ./scripts/build.sh $SERVICE_NAME" >&2
  exit 1
fi

if [[ ! -f "$ENV_DEST" ]]; then
  echo "error: config not found at $ENV_DEST" >&2
  echo "Run first: ./scripts/build.sh $SERVICE_NAME" >&2
  exit 1
fi

echo "Installing systemd service '$SERVICE_NAME' ..."

if ! id "$SERVICE_USER" &>/dev/null; then
  useradd --system --home "$BASE_PATH" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$BASE_PATH"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$SERVICE_DIR"
chmod 0750 "$ENV_DEST" || true

UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
cat > "$UNIT_PATH" <<EOF
[Unit]
Description=Subtensor Full Node ($SERVICE_NAME)
Documentation=https://github.com/opentensor/subtensor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${SERVICE_DIR}
Environment=SUBTENSOR_ENV_FILE=${ENV_DEST}
ExecStart=${RUN_SCRIPT}
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${BASE_PATH}
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

echo ""
echo "Installed ${SERVICE_NAME}.service"
echo "  binary:  $BIN_DEST"
echo "  config:  $ENV_DEST"
echo "  data:    $BASE_PATH"
echo ""
echo "Edit config:  sudo nano $ENV_DEST"
echo "Start:        sudo systemctl start $SERVICE_NAME"
echo "Logs:         sudo journalctl -u $SERVICE_NAME -f"

if [[ "$START_AFTER" == true ]]; then
  echo ""
  echo "Starting $SERVICE_NAME ..."
  systemctl start "$SERVICE_NAME"
  systemctl --no-pager status "$SERVICE_NAME" || true
fi
