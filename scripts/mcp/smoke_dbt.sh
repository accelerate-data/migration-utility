#!/usr/bin/env bash
set -euo pipefail

SETTINGS_CMD="$(python3 scripts/mcp/read_mcp_cmd.py dbt_mcp)"
SERVER_CMD="${MCP_DBT_SERVER_CMD:-$SETTINGS_CMD}"
SETTINGS_DBT_PATH="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DBT_PATH)"
SETTINGS_PROJECT_DIR="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DBT_PROJECT_DIR)"
SETTINGS_PROFILES_DIR="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DBT_PROFILES_DIR)"
SETTINGS_DISABLE_SEMANTIC_LAYER="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DISABLE_SEMANTIC_LAYER)"
SETTINGS_DISABLE_DISCOVERY="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DISABLE_DISCOVERY)"
SETTINGS_DISABLE_SQL="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DISABLE_SQL)"
SETTINGS_DISABLE_ADMIN_API="$(python3 scripts/mcp/read_mcp_env.py dbt_mcp DISABLE_ADMIN_API)"
DBT_BIN="${MCP_DBT_PATH:-${SETTINGS_DBT_PATH:-dbt}}"
PROJECT_DIR="${MCP_DBT_PROJECT_DIR:-${SETTINGS_PROJECT_DIR:-}}"
PROFILES_DIR="${MCP_DBT_PROFILES_DIR:-${SETTINGS_PROFILES_DIR:-.dbt}}"

if [[ -z "${SERVER_CMD:-}" ]]; then
  echo "MCP_DBT_SERVER_CMD is required" >&2
  echo "Set MCP_DBT_SERVER_CMD or define mcpServers.dbt_mcp in settings.local" >&2
  exit 1
fi
read -r -a SERVER_CMD_PARTS <<<"${SERVER_CMD}"
SERVER_BIN="${SERVER_CMD_PARTS[0]:-}"
if [[ -z "${SERVER_BIN}" ]]; then
  echo "Unable to parse dbt MCP command: '${SERVER_CMD}'" >&2
  exit 1
fi
if ! command -v "${SERVER_BIN}" >/dev/null 2>&1; then
  echo "dbt MCP command not found: '${SERVER_BIN}'" >&2
  echo "Install the tool or override MCP_DBT_SERVER_CMD." >&2
  exit 1
fi
if [[ -z "${PROJECT_DIR:-}" ]]; then
  echo "dbt project dir is required." >&2
  echo "Set MCP_DBT_PROJECT_DIR or define dbt_mcp.env.DBT_PROJECT_DIR in settings.local." >&2
  exit 1
fi
if [[ "${DBT_BIN}" == */* ]]; then
  if [[ ! -x "${DBT_BIN}" && -x "agent-sources/workspace/${DBT_BIN}" ]]; then
    DBT_BIN="agent-sources/workspace/${DBT_BIN}"
  fi
  if [[ ! -x "${DBT_BIN}" ]]; then
    echo "dbt executable path not found or not executable: '${DBT_BIN}'" >&2
    echo "Set MCP_DBT_PATH to a valid executable, or use the default docker wrapper from settings.local." >&2
    exit 1
  fi
elif ! command -v "${DBT_BIN}" >/dev/null 2>&1; then
  echo "dbt binary not found: '${DBT_BIN}'" >&2
  echo "Install dbt or set MCP_DBT_PATH (or dbt_mcp.env.DBT_PATH) to a valid executable." >&2
  exit 1
fi

if [[ "${DBT_BIN}" == *"dbt-docker.sh" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "Docker CLI not found while using docker-backed DBT_PATH: '${DBT_BIN}'." >&2
    echo "Install/start Docker Desktop. See docs/reference/setup-docker/README.md" >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running while using docker-backed DBT_PATH: '${DBT_BIN}'." >&2
    echo "Start Docker Desktop and retry. See docs/reference/setup-docker/README.md" >&2
    exit 1
  fi
fi
if [[ ! -d "${PROJECT_DIR}" ]]; then
  echo "dbt project directory not found: '${PROJECT_DIR}'" >&2
  exit 1
fi
if [[ ! -f "${PROJECT_DIR}/dbt_project.yml" ]]; then
  echo "Missing dbt_project.yml in '${PROJECT_DIR}'" >&2
  echo "This repo/worktree does not contain a dbt project by default." >&2
  echo "Run with a real dbt project path, for example:" >&2
  echo "  MCP_DBT_PROJECT_DIR=/absolute/path/to/dbt-project ./scripts/mcp/smoke_dbt.sh" >&2
  exit 1
fi
if [[ ! -d "${PROFILES_DIR}" ]]; then
  echo "dbt profiles directory not found: '${PROFILES_DIR}'" >&2
  echo "Set MCP_DBT_PROFILES_DIR or create the directory." >&2
  exit 1
fi
if [[ ! -f "${PROFILES_DIR}/profiles.yml" ]]; then
  echo "Missing profiles.yml in '${PROFILES_DIR}'" >&2
  echo "Set MCP_DBT_PROFILES_DIR to a valid dbt profiles directory." >&2
  exit 1
fi

export DBT_PROJECT_DIR="${PROJECT_DIR}"
export DBT_PROFILES_DIR="${PROFILES_DIR}"
export DBT_PATH="${DBT_BIN}"
if [[ -z "${DISABLE_SEMANTIC_LAYER:-}" && -n "${SETTINGS_DISABLE_SEMANTIC_LAYER}" ]]; then
  export DISABLE_SEMANTIC_LAYER="${SETTINGS_DISABLE_SEMANTIC_LAYER}"
fi
if [[ -z "${DISABLE_DISCOVERY:-}" && -n "${SETTINGS_DISABLE_DISCOVERY}" ]]; then
  export DISABLE_DISCOVERY="${SETTINGS_DISABLE_DISCOVERY}"
fi
if [[ -z "${DISABLE_SQL:-}" && -n "${SETTINGS_DISABLE_SQL}" ]]; then
  export DISABLE_SQL="${SETTINGS_DISABLE_SQL}"
fi
if [[ -z "${DISABLE_ADMIN_API:-}" && -n "${SETTINGS_DISABLE_ADMIN_API}" ]]; then
  export DISABLE_ADMIN_API="${SETTINGS_DISABLE_ADMIN_API}"
fi

echo "Checking direct dbt parse connectivity and profile validity..."
if ! "${DBT_BIN}" parse --project-dir "${PROJECT_DIR}" --profiles-dir "${PROFILES_DIR}" >/tmp/dbt-smoke-parse.log 2>&1; then
  echo "dbt parse preflight failed." >&2
  echo "Check project path/profiles/adapters. Last output:" >&2
  tail -n 40 /tmp/dbt-smoke-parse.log >&2 || true
  exit 1
fi
echo "Direct dbt parse preflight passed."

PARSE_TOOL="${MCP_DBT_PARSE_TOOL:-parse}"
COMPILE_TOOL="${MCP_DBT_COMPILE_TOOL:-compile}"

if [[ -n "${MCP_DBT_COMPILE_SELECT:-}" ]]; then
  TOOL_CALLS="[{\"name\":\"${PARSE_TOOL}\",\"arguments\":{\"project_dir\":\"${PROJECT_DIR}\"}},{\"name\":\"${COMPILE_TOOL}\",\"arguments\":{\"project_dir\":\"${PROJECT_DIR}\",\"select\":\"${MCP_DBT_COMPILE_SELECT}\"}}]"
else
  TOOL_CALLS="[{\"name\":\"${PARSE_TOOL}\",\"arguments\":{\"project_dir\":\"${PROJECT_DIR}\"}},{\"name\":\"${COMPILE_TOOL}\",\"arguments\":{\"project_dir\":\"${PROJECT_DIR}\"}}]"
fi

python3 scripts/mcp/smoke_mcp_server.py \
  --server-cmd "$SERVER_CMD" \
  --tool-calls-json "$TOOL_CALLS"
