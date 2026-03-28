---
name: init-ad-migration
description: Checks that uv, Python 3.11+, shared package deps, ddl_mcp server, and genai-toolbox are installed. Installs anything missing after user confirmation.
---

# Initialize ad-migration plugin

Verify and set up all prerequisites before using `discover`, `scope`, `/setup-ddl`,
or the `scoping-agent`.

## Step 1: Gather evidence

Run all checks **silently** — do NOT install or change anything yet.

1. `uv --version` — is uv installed?
2. `python3 --version` — is Python ≥ 3.11?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" python3 -c "import pydantic, sqlglot, typer"` — are shared package deps synced?
4. `uv run "${CLAUDE_PLUGIN_ROOT}/ddl_mcp/server.py" --help` — does the DDL MCP server start cleanly?
5. `toolbox --version` — is the genai-toolbox binary installed?
6. Check whether each of the four MSSQL environment variables is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.

If `CLAUDE_PLUGIN_ROOT` is not set, stop immediately and tell the user to load the
plugin with `claude --plugin-path <path-to-ad-migration>` before running this command.

## Step 2: Present plan

Show the user what was found and what needs to be done:

```text
Plugin status:
  uv:          ✓ installed (x.y.z)  /  ✗ not found
  python:      ✓ 3.x.x              /  ✗ not found or < 3.11
  shared deps: ✓ synced             /  ✗ not synced
  ddl_mcp:     ✓ starts             /  ✗ fails
  toolbox:     ✓ installed (x.y.z)  /  — not found (optional)

  SQL Server credentials (required for /setup-ddl and live-DB skills):
  MSSQL_HOST:  ✓ set  /  — not set
  MSSQL_PORT:  ✓ set  /  — not set
  MSSQL_DB:    ✓ set  /  — not set
  SA_PASSWORD: ✓ set  /  — not set

Actions needed:
  1. Install uv             (if missing)
  2. Install Python 3.11+   (if missing — manual step, see below)
  3. uv sync shared/        (if deps not synced)
  4. Check ddl_mcp output   (if ddl_mcp fails after sync)
```

`toolbox` and the MSSQL credentials are marked `—` (not `✗`) when missing — they
are optional for DDL file mode but required for `/setup-ddl` and any live-database
skill. They will not block setup of the core tools.

If any MSSQL variable is unset, tell the user to export them in their shell before
running `claude`, for example:

```bash
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=AdventureWorksDW
export SA_PASSWORD=<your-password>
```

These values are passed to the `mssql` MCP server at startup via environment
inheritance — they must be set in the shell before launching `claude`, not after.

If everything is already set up, say so and skip to Step 4. Otherwise, ask the user
to confirm before proceeding.

## Step 3: Execute

Only after the user confirms, run the needed actions:

**Install uv** (if missing):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installing, re-run `uv --version` to confirm. Tell the user to restart their
shell if the command is not found after install.

**Python missing**: cannot auto-install. Tell the user to install Python 3.11+ from
`https://python.org/downloads` and re-run `/init-ad-migration` after installing.
Stop here for this check — do not proceed to shared deps without Python.

**Sync shared deps** (if not synced):

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/shared"
```

**ddl_mcp fails** (after shared sync): re-run the ddl_mcp check. If it still fails,
show the error output to the user and tell them to check their Python environment.

**toolbox missing** (if the user asks how to install it):
Direct the user to `https://github.com/googleapis/genai-toolbox/releases` to download
the binary for their platform and add it to PATH. Do not attempt to install it
automatically.

## Step 4: Report

Re-run the same 6 checks and show the updated status table. Tell the user:

- **All required deps OK, credentials set, toolbox installed**: ready to use
  `discover`, `scope`, `scoping-agent`, and `/setup-ddl`.
- **toolbox missing or credentials unset**: DDL file mode (`discover`, `scope`,
  `scoping-agent`) is fully available. Live-database skills (`/setup-ddl`) require
  both `toolbox` and all four MSSQL env vars — export them in the shell before
  launching `claude`, then install `toolbox` from the genai-toolbox releases page.
- **Anything still failing**: which step to fix next before continuing.
