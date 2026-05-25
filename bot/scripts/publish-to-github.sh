#!/usr/bin/env bash
# Publish bot/ folder to DevMaster4U/subtensor-bot (bot-origin/main).
# Run from the subtensor monorepo root on branch bot-test.
# Requires: git remote add bot-origin https://github.com/DevMaster4U/subtensor-bot.git
set -euo pipefail

MSG="${1:-Update subtensor-bot}"
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

BOT_REMOTE="${BOT_REMOTE:-bot-origin}"
if ! git remote get-url "$BOT_REMOTE" &>/dev/null; then
	echo "error: missing remote '$BOT_REMOTE' — run:" >&2
	echo "  git remote add bot-origin https://github.com/DevMaster4U/subtensor-bot.git" >&2
	exit 1
fi

CURRENT="$(git branch --show-current)"
if [[ "$CURRENT" != "bot-test" ]]; then
	echo "warning: expected branch bot-test, on $CURRENT" >&2
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
	echo "error: commit or stash changes before publishing" >&2
	exit 1
fi

git checkout subtensor-bot-main
git checkout bot-test -- bot/
git add bot/

if git diff --cached --quiet; then
	echo "nothing to publish"
	git checkout bot-test
	exit 0
fi

git commit -m "$MSG"
git push "$BOT_REMOTE" subtensor-bot-main:main
git checkout bot-test

echo "published to https://github.com/DevMaster4U/subtensor-bot (main)"
