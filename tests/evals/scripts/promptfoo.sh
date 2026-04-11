#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
NODE_BIN="${npm_node_execpath:-$(command -v node)}"

exec "$NODE_BIN" "$SCRIPT_DIR/node_modules/promptfoo/dist/src/entrypoint.js" "$@"
