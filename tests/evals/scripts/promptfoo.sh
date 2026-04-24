#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
NODE_BIN="${npm_node_execpath:-$(command -v node)}"
OPENCODE_HOST="${PROMPTFOO_OPENCODE_HOST:-127.0.0.1}"
OPENCODE_PORT="${PROMPTFOO_OPENCODE_PORT:-4096}"
OPENCODE_BASE_URL="http://${OPENCODE_HOST}:${OPENCODE_PORT}"
OPENCODE_MANAGE="${PROMPTFOO_MANAGE_OPENCODE:-1}"
OPENCODE_LOG="$SCRIPT_DIR/.promptfoo/opencode-server.log"

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
export PROMPTFOO_OPENCODE_BASE_URL="$OPENCODE_BASE_URL"
export CLAUDE_PLUGIN_ROOT="$REPO_ROOT"
export TMPDIR="$SCRIPT_DIR/.tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"

cleanup() {
  if [ -n "${OPENCODE_PID:-}" ] && kill -0 "$OPENCODE_PID" >/dev/null 2>&1; then
    kill "$OPENCODE_PID" >/dev/null 2>&1 || true
    wait "$OPENCODE_PID" >/dev/null 2>&1 || true
  fi
}

is_opencode_ready() {
  curl -fsS "${OPENCODE_BASE_URL}/" >/dev/null 2>&1
}

if [ "${OPENCODE_MANAGE}" = "1" ] && ! is_opencode_ready; then
  : >"$OPENCODE_LOG"
  opencode serve --hostname "$OPENCODE_HOST" --port "$OPENCODE_PORT" >>"$OPENCODE_LOG" 2>&1 &
  OPENCODE_PID=$!
  trap cleanup EXIT INT TERM HUP

  ATTEMPT=0
  until is_opencode_ready; do
    if ! kill -0 "$OPENCODE_PID" >/dev/null 2>&1; then
      echo "OpenCode exited before becoming ready; see $OPENCODE_LOG" >&2
      exit 1
    fi
    ATTEMPT=$((ATTEMPT + 1))
    if [ "$ATTEMPT" -ge 60 ]; then
      echo "Timed out waiting for OpenCode at $OPENCODE_BASE_URL; see $OPENCODE_LOG" >&2
      exit 1
    fi
    sleep 1
  done
fi

exec "$NODE_BIN" "$SCRIPT_DIR/scripts/run-promptfoo-with-guard.js" "$@"
