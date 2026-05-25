#!/usr/bin/env bash
# Regenerate bot/node-integration.patch from bot-test vs upstream OpenTensor main.
# Run from the subtensor monorepo root.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

git fetch upstream main

git diff upstream/main..bot-test -- \
	Cargo.toml \
	node/Cargo.toml \
	node/src/lib.rs \
	node/src/service.rs \
	node/src/bot_block_announce.rs \
	> bot/node-integration.patch

echo "wrote bot/node-integration.patch ($(wc -l < bot/node-integration.patch) lines)"
echo "commit it, then run: ./bot/scripts/publish-to-github.sh \"Update patch\""
