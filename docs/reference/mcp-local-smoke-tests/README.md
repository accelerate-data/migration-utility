# Local MCP Smoke Tests

Standalone local smoke checks for developer machines.

These are intended for validating MCP server setup before wiring into agent execution.

## Scripts

- `scripts/mcp/smoke_sql_server.sh`
- `scripts/mcp/smoke_dbt.sh`
- `scripts/mcp/smoke_filesystem.sh`
- `scripts/mcp/smoke_mcp_server.py` (shared JSON-RPC helper)

## SQL Server MCP

Command source:

- Default: `mcpServers.sql_mcp` from `.claude/settings.local` (or `agent-sources/workspace/.claude/settings.local`)
- Override: `MCP_SQL_SERVER_CMD`

Optional:

- `MCP_SQL_QUERY_TOOL` — tool name for smoke call (default `describe_entities` for DAB SQL MCP)
- `MCP_SQL_TOOL_ARGS_JSON` — optional raw JSON object for tool arguments (defaults based on tool name)
- `MCP_SQL_SMOKE_QUERY` — read-only query string used only for query-style tool names
- `MCP_SQL_DOCKER_CONTAINER` — SQL Server container name (default `aw-sql`)
- `MCP_SQL_SA_PASSWORD` — SQL Server SA password used for pre-flight connectivity probe (default `P@ssw0rd123`)
- `MCP_SQL_CONNECTIVITY_QUERY` — direct probe query run via `sqlcmd` in the container before MCP smoke call
- `MCP_SQL_HOST` / `MCP_SQL_PORT` — host/port used to build default `MSSQL_CONNECTION_STRING` for DAB startup (defaults `localhost`, `1433`)

Run:

```bash
./scripts/mcp/smoke_sql_server.sh
```

Behavior:

- Verifies Docker CLI is installed and daemon is running.
- Verifies SQL Server container exists; starts it if stopped.
- Runs direct `sqlcmd` connectivity probe in the container.
- Exports deterministic `MSSQL_CONNECTION_STRING` for the smoke run (override via `MCP_SQL_CONNECTION_STRING`).
- Runs MCP initialize/list-tools/tool-call smoke flow only after direct SQL probe passes.
- If the script started the SQL container, it stops it on exit (idempotent reruns).

## dbt MCP

Command source:

- Default: `mcpServers.dbt_mcp` from `.claude/settings.local` (or `agent-sources/workspace/.claude/settings.local`)
- Override: `MCP_DBT_SERVER_CMD`
- Default `DBT_PATH`: `.claude/bin/dbt-docker.sh` (Docker-backed dbt wrapper)

Why DBT_PATH differs from SQL MCP:

- `sql_mcp` command launches the SQL MCP server process via DAB start mode (`dab start --mcp-stdio -c .claude/dab-config.json ...`).
- `dbt_mcp` also launches an MCP server directly (`uvx dbt-mcp`), but dbt command execution behind that server is controlled by `DBT_PATH`.
- Setting `DBT_PATH` to `.claude/bin/dbt-docker.sh` keeps dbt execution containerized while dbt-mcp remains host-side.

Setup note:

- Follow `docs/reference/setup-docker/README.md` for Docker-based dbt setup.
- This smoke flow uses Docker-backed dbt execution via `DBT_PATH=.claude/bin/dbt-docker.sh`.
- `dbt-mcp` still runs on host (`uvx dbt-mcp`).

Required env vars:

- None when `dbt_mcp.env.DBT_PROJECT_DIR` is configured in settings.
- Set `MCP_DBT_PROJECT_DIR` to override dbt project path per run.

Optional:

- `MCP_DBT_PARSE_TOOL` — parse tool name (default `parse`)
- `MCP_DBT_COMPILE_TOOL` — compile tool name (default `compile`)
- `MCP_DBT_COMPILE_SELECT` — optional selector for compile smoke call
- `MCP_DBT_PATH` — dbt executable (default from settings `dbt_mcp.env.DBT_PATH`)
- `MCP_DBT_PROFILES_DIR` — dbt profiles directory for preflight and smoke call setup (default from settings `dbt_mcp.env.DBT_PROFILES_DIR`, else `.dbt`)

Run:

```bash
./scripts/mcp/smoke_dbt.sh
```

Behavior:

- Validates dbt MCP command exists.
- Validates dbt executable exists (`DBT_PATH` from settings or override).
  If `DBT_PATH` is relative (for example `.claude/bin/dbt-docker.sh`), smoke test also checks `agent-sources/workspace/<DBT_PATH>`.
- If docker-backed `DBT_PATH` is used, validates Docker CLI + daemon and points to Docker setup docs on failure.
- Validates resolved dbt project directory contains `dbt_project.yml`.
- Validates `MCP_DBT_PROFILES_DIR` contains `profiles.yml`.
- Runs direct `dbt parse` preflight before MCP smoke calls.

## Filesystem MCP

Command source:

- Default: `mcpServers.filesystem` from `.claude/settings.local` (or `agent-sources/workspace/.claude/settings.local`)
- Override: `MCP_FILESYSTEM_SERVER_CMD`

Optional:

- `MCP_FILESYSTEM_READ_TOOL` — read tool name (default `read_file`)
- `MCP_FILESYSTEM_TMP_ROOT` — temp root used for smoke file creation (default `.local/mcp-smoke`)

Run:

```bash
./scripts/mcp/smoke_filesystem.sh
```

## Notes

- The helper script validates `initialize`, `notifications/initialized`, and `tools/list` for every server.
- Each smoke script also runs one representative `tools/call`.
- If your MCP tool names differ, set the `*_TOOL` env var to match your server.
- Filesystem smoke creates files under repo-local temp root so access stays within configured allowed roots.
