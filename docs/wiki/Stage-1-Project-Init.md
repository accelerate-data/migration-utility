# Stage 1 -- Project Init

The `/init-ad-migration` command verifies prerequisites, installs missing dependencies, scaffolds project files, and hands off to DDL extraction. This is the entry point for every new migration project.

## What It Checks

The command runs all checks silently before presenting results:

| Check | Required | What it verifies |
|---|---|---|
| `uv --version` | Yes | uv package manager is installed |
| `python3 --version` | Yes | Python >= 3.11 is available |
| `uv run ... python3 -c "import pydantic, sqlglot, typer"` | Yes | Shared package dependencies are synced |
| `uv run .../server.py --help` | Yes | DDL MCP server starts cleanly |
| `toolbox --version` | Optional | genai-toolbox binary for live DB skills |
| MSSQL env vars (`MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`) | Optional | SQL Server connectivity for `/setup-ddl` and live-DB skills |
| `git rev-parse --is-inside-work-tree` | Recommended | Project folder is under version control |
| `direnv version` | Recommended | direnv is installed for credential management |

## Status Display

After gathering evidence, the command presents a status table:

```text
Plugin status:
  uv:          ok installed (x.y.z)  /  not found
  python:      ok 3.x.x              /  not found or < 3.11
  shared deps: ok synced             /  not synced
  ddl_mcp:     ok starts             /  fails
  toolbox:     ok installed (x.y.z)  /  not found (optional)
  direnv:      ok installed (x.y.z)  /  not found (recommended)
  git:         ok repo detected      /  not a git repo (recommended)

  SQL Server credentials (required for /setup-ddl and live-DB skills):
  MSSQL_HOST:  ok set  /  not set
  MSSQL_PORT:  ok set  /  not set
  MSSQL_DB:    ok set  /  not set
  SA_PASSWORD: ok set  /  not set

Actions needed:
  1. Install uv             (if missing)
  2. Install Python 3.11+   (if missing)
  3. uv sync shared/        (if deps not synced)
  4. Check ddl_mcp output   (if ddl_mcp fails after sync)
```

Optional items (`toolbox`, `direnv`, MSSQL credentials) are marked with a dash when missing, not a failure marker. They do not block core setup.

## What It Scaffolds

After the user confirms, the command runs the `init` CLI to create project files:

| File | Purpose |
|---|---|
| `CLAUDE.md` | Project instructions for Claude Code sessions |
| `README.md` | Human-readable project overview |
| `repo-map.json` | Structure, entrypoints, modules, and commands for agent context |
| `.gitignore` | Standard ignores including `.migration-runs/`, `.staging/`, `.envrc` |
| `.envrc` | direnv template for MSSQL credentials (gitignored) |
| `.claude/rules/git-workflow.md` | Worktree and PR conventions |
| `.githooks/pre-commit` | Pre-commit hook for validation |

The scaffold runs two CLI commands:

```bash
uv run --project <shared-path> init scaffold-project --project-root .
uv run --project <shared-path> init scaffold-hooks --project-root .
```

If the working directory is a git repository, the command commits the scaffolded files (excluding `.envrc`, which is gitignored).

After committing, the command tells you to restart Claude to pick up the new project instructions in `CLAUDE.md`.

## Idempotency

Safe to re-run at any time. Each step checks current state before acting:

- Prerequisites in the check phase re-evaluate actual environment state
- The `init` CLI is fully idempotent: existing `CLAUDE.md` is checked for missing sections (not overwritten), `README.md` and `repo-map.json` are skipped if present, `.gitignore` gets only missing entries appended, `.envrc` is skipped if present
- The commit step only runs if there are staged changes

## Handoff

After scaffolding, the command provides next-step guidance based on your environment:

- **toolbox installed and all MSSQL vars set**: ready to run `/setup-ddl` to extract DDL from the live database
- **toolbox missing or MSSQL vars unset**: DDL file mode is available. Live-database skills require both `toolbox` and all four MSSQL env vars. If using direnv, fill in `.envrc` and run `direnv allow`, then install `toolbox` from the genai-toolbox releases page.

## Next Step

Proceed to [[Stage 2 DDL Extraction]] to extract schema and catalog data from your source database.
