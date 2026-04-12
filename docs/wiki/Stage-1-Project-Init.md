# Stage 1 -- Project Init

The `/init-ad-migration` command verifies prerequisites, installs missing dependencies, scaffolds project files, and hands off to DDL extraction. This is the entry point for every new migration project. It is source-technology-aware — pass a source name to skip the prompt, or omit it to be guided through selection.

Windows is not supported for the local init flow. The command stops immediately on Windows instead of attempting partial setup.

```text
/init-ad-migration           -- prompts for source selection
/init-ad-migration oracle    -- skips prompt, configures for Oracle
```

Supported sources: `mssql` (SQL Server, default) and `oracle`.

## What It Checks

Checks are grouped into common (all sources) and source-specific sections.

### Common checks (all sources)

| Check | Required | What it verifies |
|---|---|---|
| `uv --version` | Yes | uv package manager is installed |
| `python3 --version` | Yes | Python >= 3.11 is available |
| `uv run ... python3 -c "import pydantic, sqlglot, typer"` | Yes | Shared package dependencies are synced |
| `uv run .../server.py --help` | Yes | DDL MCP server starts cleanly |
| `git rev-parse --is-inside-work-tree` | Recommended | Project folder is under version control |
| `direnv version` | Recommended | direnv is installed for credential management |

### SQL Server-specific checks

| Check | Required | What it verifies |
|---|---|---|
| `uv run ... init check-freetds` | Yes | Homebrew FreeTDS is installed, `odbcinst` is available, and `FreeTDS` is registered in unixODBC |
| `toolbox --version` | Optional | genai-toolbox binary for live DB skills |
| `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` | Optional | SQL Server connectivity for `/setup-ddl` and live-DB skills |

### Oracle-specific checks

| Check | Required | What it verifies |
|---|---|---|
| `sql -V` (SQLcl) | Optional | SQLcl binary for Oracle MCP server |
| Java 11+ (`java -version`) | Optional | Required by SQLcl |
| `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD` | Optional | Oracle connectivity for live-DB skills |

## Status Display

After gathering evidence, the command presents a status table grouped by source:

```text
Plugin status:

  Common:
  uv:          ok installed (x.y.z)  /  not found
  python:      ok 3.x.x              /  not found or < 3.11
  shared deps: ok synced             /  not synced
  ddl_mcp:     ok starts             /  fails
  direnv:      ok installed (x.y.z)  /  not found (recommended)
  git:         ok repo detected      /  not a git repo (recommended)

  SQL Server-specific:
  freetds:     ok installed + registered  /  not installed  /  unixODBC missing  /  not registered
  toolbox:     ok installed (x.y.z)  /  not found (optional)
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

Optional items are marked with a dash when missing, not a failure marker. They do not block core setup.

## What It Scaffolds

After the user confirms, the command runs the `scaffold-project` CLI with the selected source technology:

```bash
uv run --project <shared-path> init scaffold-project --project-root . --technology <mssql|oracle>
uv run --project <shared-path> init scaffold-hooks --project-root .
```

| File | Purpose |
|---|---|
| `CLAUDE.md` | Project instructions for Claude Code sessions, including source-specific MCP runtime notes |
| `README.md` | Human-readable project overview |
| `repo-map.json` | Structure, entrypoints, modules, and commands for agent context |
| `.gitignore` | Standard ignores including `.migration-runs/`, `.staging/`, `.envrc` |
| `.envrc` | direnv template with env vars for the selected source only (gitignored) |
| `.claude/rules/git-workflow.md` | Worktree and PR conventions |
| `.githooks/pre-commit` | Pre-commit hook for validation |

The scaffolded `CLAUDE.md` includes source-specific runtime instructions — for Oracle, this includes the manual MCP connect step required at the start of each session.

The scaffolded `.envrc` contains only the env vars for the selected source. SQL Server gets `MSSQL_*` vars; Oracle gets `ORACLE_*` vars.

The init command also writes a partial `manifest.json` with `technology` and `dialect` — `/setup-ddl` enriches it later with `database` and `schemas`.

If the working directory is a git repository, the command commits the scaffolded files (excluding `.envrc`, which is gitignored).

After committing, the command tells you to restart Claude to pick up the new project instructions in `CLAUDE.md`.

## Idempotency

Safe to re-run at any time. Each step checks current state before acting:

- Prerequisites in the check phase re-evaluate actual environment state
- The `init` CLI is fully idempotent: existing `CLAUDE.md` is checked for missing sections (not overwritten), `README.md` and `repo-map.json` are skipped if present, `.gitignore` gets only missing entries appended, `.envrc` is skipped if present
- The commit step only runs if there are staged changes

## Handoff

After scaffolding, the command provides next-step guidance based on your environment:

- **toolbox installed and all source vars set**: ready to run `/setup-ddl` to extract DDL from the live database
- **toolbox missing or vars unset**: DDL file mode is available. Live-database skills require both `toolbox` and all source-specific env vars. If using direnv, fill in `.envrc` and run `direnv allow`, then install the required tooling.

## Next Step

Proceed to [[Stage 2 DDL Extraction]] to extract schema and catalog data from your source database.
