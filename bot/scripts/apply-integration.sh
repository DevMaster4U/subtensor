#!/usr/bin/env bash
# Idempotent bot node integration — use instead of git apply when patch fails.
# Run from subtensor repo root:
#   ./bot/scripts/apply-integration.sh
# Or from anywhere:
#   ./bot/scripts/apply-integration.sh /path/to/subtensor
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
	echo "error: python3 required" >&2
	exit 1
fi

python3 "$SCRIPT_DIR/apply-integration.py" "$ROOT"
