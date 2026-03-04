#!/usr/bin/env bash
set -euo pipefail

SETTINGS_CMD="$(python3 scripts/mcp/read_mcp_cmd.py filesystem)"
SERVER_CMD="${MCP_FILESYSTEM_SERVER_CMD:-$SETTINGS_CMD}"

if [[ -z "${SERVER_CMD:-}" ]]; then
  echo "MCP_FILESYSTEM_SERVER_CMD is required" >&2
  echo "Set MCP_FILESYSTEM_SERVER_CMD or define mcpServers.filesystem in settings.local" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
REPO_TMP_ROOT="${MCP_FILESYSTEM_TMP_ROOT:-.local/mcp-smoke}"
mkdir -p "${REPO_TMP_ROOT}"
TMP_DIR="$(mktemp -d "${REPO_TMP_ROOT}/run-XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "filesystem smoke" > "$TMP_DIR/smoke.txt"

READ_TOOL="${MCP_FILESYSTEM_READ_TOOL:-read_file}"

python3 scripts/mcp/smoke_mcp_server.py \
  --server-cmd "$SERVER_CMD" \
  --tool-calls-json "[{\"name\":\"${READ_TOOL}\",\"arguments\":{\"path\":\"${TMP_DIR}/smoke.txt\"}}]"
