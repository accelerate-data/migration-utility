---
name: init-ad-migration
description: Checks prerequisites, installs missing deps, scaffolds project files (CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .githooks), and hands off to /setup-ddl.
---

# Initialize ad-migration plugin

Verify and set up all prerequisites before using `listing-objects`, `scoping-table`, `/setup-ddl`, or the `scoping` agent. Then scaffold the project directory for both agents and human developers.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop immediately and tell the user to load the plugin with `claude --plugin-dir <path-to-ad-migration>` before running this command.

## Step 2: Gather evidence

Run all checks **silently** — do NOT install or change anything yet.

1. `uv --version` — is uv installed?
2. `python3 --version` — is Python >= 3.11?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" python3 -c "import pydantic, sqlglot, typer"` — are shared package deps synced?
4. `uv run "${CLAUDE_PLUGIN_ROOT}/../../mcp/ddl/server.py" --help` — does the DDL MCP server start cleanly?
5. `toolbox --version` — is the genai-toolbox binary installed?
6. Check whether each of the four MSSQL environment variables is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.
7. `git rev-parse --is-inside-work-tree` — is the current working directory inside a git repository? If not, warn the user that the project folder is not under version control and recommend initialising git before running extraction skills.
8. `direnv version` — is direnv installed? This is optional; mark as `—` if missing.

## Step 3: Present plan

Show the user what was found and what needs to be done:

```text
Plugin status:
  uv:          ✓ installed (x.y.z)  /  ✗ not found
  python:      ✓ 3.x.x              /  ✗ not found or < 3.11
  shared deps: ✓ synced             /  ✗ not synced
  ddl_mcp:     ✓ starts             /  ✗ fails
  toolbox:     ✓ installed (x.y.z)  /  — not found (optional)
  direnv:      ✓ installed (x.y.z)  /  — not found (recommended)
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

`toolbox`, `direnv`, and the MSSQL credentials are marked `—` (not `✗`) when missing — they are optional for DDL file mode but required for `/setup-ddl` and any live-database skill. They will not block setup of the core tools.

If any MSSQL variable is unset, recommend using direnv for credential management:

> **Recommended: use direnv for credentials.** The scaffolding step will create a `.envrc` template. Fill in your values and run `direnv allow`. This keeps credentials out of your shell history and loads them automatically when you enter the project directory.
>
> If you prefer not to use direnv, export the variables in your shell before launching `claude`:
>
> ```bash
> export MSSQL_HOST=localhost
> export MSSQL_PORT=1433
> export MSSQL_DB=AdventureWorksDW
> export SA_PASSWORD=<your-password>
> ```

These values are passed to the `mssql` MCP server at startup via environment inheritance — they must be set before launching `claude`, not after.

If everything is already set up, say so and skip Step 4 (nothing to install). Proceed directly to Step 5. Otherwise, ask the user to confirm before proceeding.

## Step 4: Execute

Only after the user confirms, run the needed actions:

**Install uv** (if missing):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installing, re-run `uv --version` to confirm. Tell the user to restart their shell if the command is not found after install.

**Python missing**: cannot auto-install. Tell the user to install Python 3.11+ from `https://python.org/downloads` and re-run `/init-ad-migration` after installing. Stop here for this check — do not proceed to shared deps without Python.

**Sync shared deps** (if not synced):

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/../../lib"
```

**ddl_mcp fails** (after shared sync): re-run the ddl_mcp check. If it still fails, show the error output to the user and tell them to check their Python environment.

**toolbox missing** (if the user asks how to install it): Direct the user to `https://github.com/googleapis/genai-toolbox/releases` to download the binary for their platform and add it to PATH. Do not attempt to install it automatically.

**direnv missing** (if the user asks how to install it): Direct them to `https://direnv.net` for install instructions. Do not attempt to install it automatically.

## Step 5: Scaffold project files

Run the `init` CLI to scaffold the project directory. This creates CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .claude/rules/git-workflow.md, and .githooks/pre-commit — all idempotently.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" init scaffold-project --project-root .
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" init scaffold-hooks --project-root .
```

Parse the JSON output and report to the user which files were created, updated, or skipped.

If `scaffold-project` reports missing CLAUDE.md sections (in `files_skipped`), tell the user which sections are missing and recommend adding them.

## Step 6: Commit

If the working directory is a git repository, commit the files created or modified in Step 5:

```bash
git add CLAUDE.md README.md .gitignore .githooks/ repo-map.json .claude/
git commit -m "chore: init migration project"
```

Do not stage `.envrc` — it is gitignored and contains credentials.

If not a git repository, skip silently.

Then tell the user: **"Restart Claude to pick up the new project instructions."**

## Step 7: Handoff

Tell the user:

- **toolbox installed and all MSSQL vars set**: ready to run `/setup-ddl` to extract DDL from the live database.
- **toolbox missing or MSSQL vars unset**: DDL file mode (`listing-objects`, `scoping-table`, `scoping`) is fully available. Live-database skills (`/setup-ddl`) require both `toolbox` and all four MSSQL env vars. If using direnv, fill in `.envrc` and run `direnv allow`. Then install `toolbox` from the genai-toolbox releases page.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `init scaffold-project` | non-zero | File IO failure. Surface error message, stop |
| `init scaffold-project` | 0 + `files_skipped` non-empty | Files already exist. Report which were skipped — not an error |
| `init scaffold-hooks` | non-zero | Hook creation or git config failed. Surface error message |
| `uv run ... python3 -c "import ..."` | non-zero | Shared deps not synced. Tell user to run `cd lib && uv sync` |

## Idempotency

Safe to re-run. Each step checks current state before acting:

- Checks in Step 2 re-evaluate actual environment state.
- Step 5 uses the `init` CLI which is fully idempotent: existing CLAUDE.md is checked for missing sections (not overwritten), README.md and repo-map.json are skipped if present, .gitignore gets only missing entries appended, .envrc is skipped if present, .claude/rules/git-workflow.md is skipped if present, .githooks/pre-commit is skipped if present.
- Step 6 only commits if there are staged changes.
