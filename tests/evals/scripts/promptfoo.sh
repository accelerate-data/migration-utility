#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
NODE_BIN="${npm_node_execpath:-$(command -v node)}"

mkdir -p \
  "$SCRIPT_DIR/.promptfoo" \
  "$SCRIPT_DIR/.cache/promptfoo" \
  "$SCRIPT_DIR/.tmp" \
  "$SCRIPT_DIR/results/logs" \
  "$SCRIPT_DIR/output/media"

export PROMPTFOO_CONFIG_DIR="$SCRIPT_DIR/.promptfoo"
export PROMPTFOO_CACHE_PATH="$SCRIPT_DIR/.cache/promptfoo"
export PROMPTFOO_LOG_DIR="$SCRIPT_DIR/results/logs"
export PROMPTFOO_MEDIA_PATH="$SCRIPT_DIR/output/media"
export CLAUDE_PLUGIN_ROOT="$REPO_ROOT"
export TMPDIR="$SCRIPT_DIR/.tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"

exec "$NODE_BIN" "$SCRIPT_DIR/scripts/run-promptfoo-with-guard.js" "$@"
