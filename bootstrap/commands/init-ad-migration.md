---
name: init-ad-migration
description: Checks that uv, Python 3.11+, shared package deps, ddl_mcp server, and genai-toolbox are installed. Installs anything missing after user confirmation.
---

# Initialize ad-migration plugin

Verify and set up all prerequisites before using `discover`, `scope`, `/setup-ddl`, or the `scoping-agent`.

## Step 0: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop immediately and tell the user to load the plugin with `claude --plugin-dir <path-to-ad-migration>` before running this command.

## Step 1: Gather evidence

Run all checks **silently** — do NOT install or change anything yet.

1. `uv --version` — is uv installed?
2. `python3 --version` — is Python ≥ 3.11?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/../lib" python3 -c "import pydantic, sqlglot, typer"` — are shared package deps synced?
4. `uv run "${CLAUDE_PLUGIN_ROOT}/../mcp/ddl/server.py" --help` — does the DDL MCP server start cleanly?
5. `toolbox --version` — is the genai-toolbox binary installed?
6. Check whether each of the four MSSQL environment variables is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.
7. `git rev-parse --is-inside-work-tree` — is the current working directory inside a git repository? If not, warn the user that the project folder is not under version control and recommend initialising git before running extraction skills.

## Step 2: Present plan

Show the user what was found and what needs to be done:

```text
Plugin status:
  uv:          ✓ installed (x.y.z)  /  ✗ not found
  python:      ✓ 3.x.x              /  ✗ not found or < 3.11
  shared deps: ✓ synced             /  ✗ not synced
  ddl_mcp:     ✓ starts             /  ✗ fails
  toolbox:     ✓ installed (x.y.z)  /  — not found (optional)
  git:         ✓ repo detected      /  — not a git repo (recommended)

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

`toolbox` and the MSSQL credentials are marked `—` (not `✗`) when missing — they are optional for DDL file mode but required for `/setup-ddl` and any live-database skill. They will not block setup of the core tools.

If any MSSQL variable is unset, tell the user to export them in their shell before
running `claude`, for example:

```bash
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=AdventureWorksDW
export SA_PASSWORD=<your-password>
```

These values are passed to the `mssql` MCP server at startup via environment inheritance — they must be set in the shell before launching `claude`, not after.

If everything is already set up, say so and skip Step 3 (nothing to install). Proceed directly to Step 5. Otherwise, ask the user to confirm before proceeding.

## Step 3: Execute

Only after the user confirms, run the needed actions:

**Install uv** (if missing):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installing, re-run `uv --version` to confirm. Tell the user to restart their shell if the command is not found after install.

**Python missing**: cannot auto-install. Tell the user to install Python 3.11+ from `https://python.org/downloads` and re-run `/init-ad-migration` after installing. Stop here for this check — do not proceed to shared deps without Python.

**Sync shared deps** (if not synced):

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/../lib"
```

**ddl_mcp fails** (after shared sync): re-run the ddl_mcp check. If it still fails,
show the error output to the user and tell them to check their Python environment.

**toolbox missing** (if the user asks how to install it): Direct the user to `https://github.com/googleapis/genai-toolbox/releases` to download the binary for their platform and add it to PATH. Do not attempt to install it automatically.

## Step 4: Report

Re-run the same 7 checks and show the updated status table. Tell the user:

- **All required deps OK, credentials set, toolbox installed**: ready to use `discover`, `scope`, `scoping-agent`, and `/setup-ddl`.
- **toolbox missing or credentials unset**: DDL file mode (`discover`, `scope`, `scoping-agent`) is fully available. Live-database skills (`/setup-ddl`) require both `toolbox` and all four MSSQL env vars — export them in the shell before launching `claude`, then install `toolbox` from the genai-toolbox releases page.
- **Anything still failing**: which step to fix next before continuing.

## Step 5: Seed project CLAUDE.md

Check whether a `CLAUDE.md` exists in the current working directory.

**If no `CLAUDE.md` exists**, write one using the template below. Tell the user it was created.

**If a `CLAUDE.md` already exists**, read it and check whether it contains the minimum required sections listed below. For any missing section, tell the user which sections are missing and recommend adding them. Do not overwrite or modify the existing file without explicit confirmation.

### Minimum required sections

Every migration project's `CLAUDE.md` should contain the following sections:

1. **Domain** — what the project is migrating (source system, target platform)
2. **Stack** — technology table (source DDL access, live DB access, transformation target, storage, orchestration, platform)
3. **Commit Discipline** — checkpoint table and commit message format

### Template for new projects

```markdown
# Migration Project

## Domain

Migrating a data warehouse to **Vibedata Managed Fabric Lakehouse**. Source system: **Microsoft SQL Server** (T-SQL stored procedures).

Migration target: silver and gold dbt transformations on the Fabric Lakehouse endpoint. Bronze ingestion layers, ADF pipelines, and Power BI are out of scope.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `.sql` files; no live DB required |
| Live source DB access | `mssql` MCP via genai-toolbox | Requires `toolbox` binary on PATH |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | |
| Platform | **Microsoft Fabric** on Azure | |

## Commit Discipline

Commit at logical checkpoints so work is never lost mid-session.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages: `type: short description` (e.g. `feat: extract DDL from AdventureWorks`).

If not a git repository, skip commit steps silently.
```

## Step 6: Set up .gitignore

If the working directory is inside a git repository (confirmed by the git check in Step 1), check whether a `.gitignore` file exists.

**If no `.gitignore` exists**, create one with the base entries below.

**If a `.gitignore` already exists**, read it and append any missing entries from the list below. Do not duplicate entries that are already present.

### Required .gitignore entries

```text
# Staging files from setup-ddl (intermediate MCP query results)
.staging/

# Python
__pycache__/
*.pyc
.venv/

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.*
!.env.example
```

Tell the user which entries were added (or that `.gitignore` was created).

## Step 7: Commit

If the working directory is a git repository, commit the files created or modified in Steps 5 and 6:

```bash
git add CLAUDE.md .gitignore
git commit -m "chore: init migration project (CLAUDE.md, .gitignore)"
```

If not a git repository, skip silently.

Then tell the user: **"Restart Claude to pick up the new project instructions."**

## Idempotency

Safe to re-run. Each step checks current state before acting:

- Checks in Step 1 re-evaluate actual environment state.
- Step 5 reads existing `CLAUDE.md` and checks for missing sections rather than overwriting.
- Step 6 appends only missing `.gitignore` entries.
- Step 7 only commits if there are staged changes.
