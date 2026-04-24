#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
NODE_BIN="${npm_node_execpath:-$(command -v node)}"
OPENCODE_XDG_ROOT="$SCRIPT_DIR/.promptfoo/opencode-runtime"

mkdir -p \
  "$SCRIPT_DIR/.promptfoo" \
  "$SCRIPT_DIR/.cache/promptfoo" \
  "$SCRIPT_DIR/.tmp" \
  "$SCRIPT_DIR/results/logs" \
  "$SCRIPT_DIR/output/media"

mkdir -p "$OPENCODE_XDG_ROOT/state"

export PROMPTFOO_CONFIG_DIR="$SCRIPT_DIR/.promptfoo"
export PROMPTFOO_CACHE_PATH="$SCRIPT_DIR/.cache/promptfoo"
export PROMPTFOO_LOG_DIR="$SCRIPT_DIR/results/logs"
export PROMPTFOO_MEDIA_PATH="$SCRIPT_DIR/output/media"
export PROMPTFOO_EVAL_TIMEOUT_MS="${PROMPTFOO_EVAL_TIMEOUT_MS:-900000}"
export PROMPTFOO_SCHEDULER_QUEUE_TIMEOUT_MS="${PROMPTFOO_SCHEDULER_QUEUE_TIMEOUT_MS:-900000}"
export CLAUDE_PLUGIN_ROOT="$REPO_ROOT"
export TMPDIR="$SCRIPT_DIR/.tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export XDG_STATE_HOME="$OPENCODE_XDG_ROOT/state"

exec "$NODE_BIN" "$SCRIPT_DIR/scripts/run-promptfoo-with-guard.js" "$@"
