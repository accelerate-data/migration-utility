#!/usr/bin/env bash
set -euo pipefail

SETTINGS_CMD="$(python3 scripts/mcp/read_mcp_cmd.py sql_mcp)"
SERVER_CMD="${MCP_SQL_SERVER_CMD:-$SETTINGS_CMD}"
CONTAINER_NAME="${MCP_SQL_DOCKER_CONTAINER:-aw-sql}"
SA_PASSWORD="${MCP_SQL_SA_PASSWORD:-P@ssw0rd123}"
CONNECT_QUERY="${MCP_SQL_CONNECTIVITY_QUERY:-SELECT TOP 1 name FROM sys.databases ORDER BY name;}"
SQL_HOST="${MCP_SQL_HOST:-localhost}"
SQL_PORT="${MCP_SQL_PORT:-1433}"
STARTED_BY_SCRIPT=0

cleanup() {
  if [[ "${STARTED_BY_SCRIPT}" -eq 1 ]]; then
    echo "Stopping SQL Server container '${CONTAINER_NAME}' (started by this script)..."
    docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "${SERVER_CMD:-}" ]]; then
  echo "MCP_SQL_SERVER_CMD is required" >&2
  echo "Set MCP_SQL_SERVER_CMD or define mcpServers.sql_mcp in settings.local" >&2
  exit 1
fi
read -r -a SERVER_CMD_PARTS <<<"${SERVER_CMD}"
SERVER_BIN="${SERVER_CMD_PARTS[0]:-}"
if [[ -z "${SERVER_BIN}" ]]; then
  echo "Unable to parse SQL MCP command: '${SERVER_CMD}'" >&2
  exit 1
fi
if [[ "${SERVER_BIN}" == "dab" ]]; then
  DAB_PATH="$(command -v dab 2>/dev/null || true)"
  if [[ -z "${DAB_PATH}" && -x "$HOME/.dotnet/tools/dab" ]]; then
    DAB_PATH="$HOME/.dotnet/tools/dab"
  fi
  if [[ -n "${DAB_PATH}" ]]; then
    SERVER_CMD_PARTS[0]="${DAB_PATH}"
  else
    export PATH="$HOME/.dotnet/tools:$PATH"
    DAB_PATH="$(command -v dab 2>/dev/null || true)"
    if [[ -n "${DAB_PATH}" ]]; then
      SERVER_CMD_PARTS[0]="${DAB_PATH}"
    fi
  fi
fi
SERVER_BIN="${SERVER_CMD_PARTS[0]:-}"
if ! command -v "${SERVER_BIN}" >/dev/null 2>&1; then
  echo "SQL MCP command not found: '${SERVER_BIN}'" >&2
  echo "Install the tool or override MCP_SQL_SERVER_CMD." >&2
  echo "If installed as a .NET global tool, ensure ~/.dotnet/tools is on PATH." >&2
  exit 1
fi

SERVER_CMD=""
for part in "${SERVER_CMD_PARTS[@]}"; do
  if [[ -z "${SERVER_CMD}" ]]; then
    SERVER_CMD="$(printf '%q' "$part")"
  else
    SERVER_CMD="${SERVER_CMD} $(printf '%q' "$part")"
  fi
done

if [[ "${SERVER_BIN}" == "dab" ]]; then
  if ! dab start --help 2>&1 | rg -q -- "--mcp-stdio"; then
    echo "Installed dab does not support MCP stdio mode on 'dab start' (--mcp-stdio)." >&2
    echo "Upgrade to Data API Builder v1.7+ (prerelease currently required for MCP):" >&2
    echo "  dotnet tool update --global Microsoft.DataApiBuilder --prerelease" >&2
    echo "Then verify: dab --version && dab start --help | rg -- --mcp-stdio" >&2
    exit 1
  fi
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH." >&2
  echo "See docs/reference/setup-docker/README.md" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running or not reachable." >&2
  echo "Start Docker Desktop, then re-run this script." >&2
  exit 1
fi

if ! docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  echo "SQL Server container '${CONTAINER_NAME}' not found." >&2
  echo "Create it using docs/reference/setup-docker/README.md (container name: aw-sql)." >&2
  exit 1
fi

if [[ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}")" != "true" ]]; then
  echo "Starting SQL Server container '${CONTAINER_NAME}'..."
  docker start "${CONTAINER_NAME}" >/dev/null
  STARTED_BY_SCRIPT=1
fi

echo "Checking direct SQL Server connectivity in container '${CONTAINER_NAME}'..."
probe_ok=0
for _ in $(seq 1 15); do
  if docker exec "${CONTAINER_NAME}" /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "${SA_PASSWORD}" -C -Q "${CONNECT_QUERY}" >/dev/null 2>&1; then
    probe_ok=1
    break
  fi
  sleep 2
done

if [[ "${probe_ok}" -ne 1 ]]; then
  echo "Failed SQL connectivity probe against container '${CONTAINER_NAME}'." >&2
  echo "Check SA password (MCP_SQL_SA_PASSWORD), container logs, and setup guide:" >&2
  echo "docs/reference/setup-docker/README.md" >&2
  docker logs --tail 40 "${CONTAINER_NAME}" >&2 || true
  exit 1
fi

echo "Direct SQL connectivity check passed."

# Always set a deterministic connection string for this smoke run so a stale shell
# export does not cause DAB config deserialization failures.
if [[ -n "${MCP_SQL_CONNECTION_STRING:-}" ]]; then
  export MSSQL_CONNECTION_STRING="${MCP_SQL_CONNECTION_STRING}"
else
  export MSSQL_CONNECTION_STRING="Server=${SQL_HOST},${SQL_PORT};Database=master;User ID=sa;Password=${SA_PASSWORD};TrustServerCertificate=true;Encrypt=true"
fi

QUERY="${MCP_SQL_SMOKE_QUERY:-SELECT TOP 1 name FROM sys.objects}"
TOOL_NAME="${MCP_SQL_QUERY_TOOL:-describe_entities}"
TOOL_ARGS="${MCP_SQL_TOOL_ARGS_JSON:-}"

if [[ -z "${TOOL_ARGS}" ]]; then
  case "${TOOL_NAME}" in
    describe_entities)
      TOOL_ARGS='{}'
      ;;
    query|execute_query|run_query|sql_query)
      TOOL_ARGS="{\"query\":\"${QUERY}\"}"
      ;;
    *)
      TOOL_ARGS='{}'
      ;;
  esac
fi

python3 scripts/mcp/smoke_mcp_server.py \
  --server-cmd "$SERVER_CMD" \
  --tool-calls-json "[{\"name\":\"${TOOL_NAME}\",\"arguments\":${TOOL_ARGS}}]"
