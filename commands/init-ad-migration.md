---
name: init-ad-migration
description: Checks environment prerequisites for the chosen source and target technologies, installs missing deps, scaffolds project files (CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .githooks), writes a partial manifest, and hands off to later setup stages.
user-invocable: true
---

# Initialize ad-migration plugin

Verify and set up all prerequisites before using `listing-objects`, `analyzing-table`, or `ad-migration setup-source`. Then scaffold the project directory for both agents and human developers.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track the automated phases of this command. After the user confirms (Step 4) and before execution begins, create tasks for each automated step that will run (e.g. `Install dependencies`, `Scaffold project files`, `Commit scaffolding` — only include steps that are actually needed). Update each task to `in_progress` when it starts and to `completed` or `cancelled` (include the error reason) when it finishes. Do not create tasks for interactive steps (source selection, confirmation prompts).

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop immediately and tell the user to load the plugin with `claude --plugin-dir <path-to-ad-migration>` before running this command.

If the host platform is Windows, stop immediately and tell the user local Windows execution is not supported for this workflow. Recommend running the plugin on macOS or Linux instead. Do not continue with any prerequisite checks on Windows.

## Step 1.5: Install ad-migration CLI

Check whether `ad-migration` is already on PATH:

```bash
ad-migration --version 2>/dev/null && echo "INSTALLED" || echo "NOT_FOUND"
```

If already installed, print the version and continue to Step 2.

If not installed, install via Homebrew:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
```

After installing, verify:

```bash
ad-migration --version
```

If Homebrew is not available on the user's machine, tell them:

> Install Homebrew first: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
> Then re-run `/init-ad-migration`.

Do not continue if `ad-migration --version` still fails after installation.

## Step 2: Runtime selection

Determine which source and target technologies to configure:

1. Check `$ARGUMENTS` for a positional source slug (e.g. `/init-ad-migration oracle` or `/init-ad-migration sql_server`).
2. If no source argument was provided, ask the user to choose:

> **Which source database are you migrating from?**
>
> 1. `sql_server` — Microsoft SQL Server (T-SQL)
> 2. `oracle` — Oracle Database (PL/SQL)

1. Ask the user which target technology they want to generate dbt assets for:

> **Which target database technology are you writing dbt assets for?**
>
> 1. `sql_server` — Microsoft SQL Server
> 2. `oracle` — Oracle Database

Validate both chosen slugs against the source registry in `init.py`. If either slug is unknown, list the valid options and ask again.

Store the chosen slugs as `$SOURCE` and `$TARGET` for the remaining steps.

Do **not** ask a separate sandbox question during init. The partial manifest persists `runtime.sandbox` as a separate role, initialized from `$SOURCE`, so later commands can still manage sandbox explicitly.

## Step 3: Gather evidence

### Read existing handoff state

Before running any checks, read `manifest.json` in the project root. If it contains an `init_handoff` key, load it as `$EXISTING_HANDOFF`. For idempotency, skip checks already recorded as passing in the matching handoff section. Re-run checks that are `false`, missing, or belong to a different selected technology.

To force a full re-check of all prerequisites, the user must delete `manifest.json` manually.

### Run only the needed checks silently

Do NOT install or change anything yet — only gather evidence for items not already validated.

### Common prerequisites (all projects)

1. `uv --version` — is uv installed?
2. `python3 --version` — is Python >= 3.11?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import pydantic, sqlglot, typer"` — are shared package deps synced?
4. `uv run "${CLAUDE_PLUGIN_ROOT}/mcp/ddl/server.py" --help` — does the DDL MCP server start cleanly?
5. `git rev-parse --is-inside-work-tree` — is the current working directory inside a git repository? If not, warn the user that the project folder is not under version control and recommend initialising git before running extraction skills.
6. `direnv version` — is direnv installed? This is optional; mark as `—` if missing.

### Source runtime prerequisites

Run the source-stack checks for `$SOURCE`. These checks validate that the local machine can support the selected source technology. Do **not** ask for or validate hostnames, database names, usernames, passwords, or other connection details during init.

#### When `$SOURCE` is `sql_server`

1. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init check-freetds` — verify that Homebrew FreeTDS is installed, `odbcinst` is available, and `FreeTDS` appears in `odbcinst -q -d`
2. `toolbox --version` — is the genai-toolbox binary installed?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init discover-mssql-driver-override` — discover the effective local SQL Server driver override. If the result is `status="resolved"`, add `MSSQL_DRIVER` to `$OVERRIDES`. If the result is `status="manual"`, show the returned `message` verbatim.

#### When `$SOURCE` is `oracle`

1. `sql -V` — is SQLcl installed?
2. `java -version` — is Java 11+ installed?
3. Verify the Oracle MCP server can start: test that `sql -mcp` or `"${SQLCL_BIN}" -mcp` exits cleanly. This is a startup check only and does **not** require a live DB connection.
4. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init discover-sqlcl-bin-override` — discover the effective local SQLcl binary path. If the result is `status="resolved"`, add `SQLCL_BIN` to `$OVERRIDES`. If the result is `status="manual"`, show the returned `message` verbatim.

### Target runtime prerequisites

Run the target-stack checks for `$TARGET`. These checks validate that the local machine has the client libraries needed by `/setup-target` and later dbops-backed flows. Do **not** ask for or validate target connection details during init.

#### When `$TARGET` is `sql_server`

1. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import pyodbc"` — is the SQL Server Python client available?
2. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init discover-mssql-driver-override` — discover the effective local SQL Server driver override. If the result is `status="resolved"` and `MSSQL_DRIVER` is not already present in `$OVERRIDES`, add it. If the result is `status="manual"`, show the returned `message` verbatim.

#### When `$TARGET` is `oracle`

1. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import oracledb"` — is the Oracle Python client available?

## Step 4: Present plan

Show the user what was found and what needs to be done, grouped into common, source-runtime, and target-runtime sections:

```text
Common prerequisites:
  uv:          ✓ installed (x.y.z)  /  ✗ not found
  python:      ✓ 3.x.x              /  ✗ not found or < 3.11
  shared deps: ✓ synced             /  ✗ not synced
  ddl_mcp:     ✓ starts             /  ✗ fails
  git:         ✓ repo detected      /  — not a git repo (recommended)
  direnv:      ✓ installed (x.y.z)  /  — not found (recommended)
```

**For source = SQL Server:**

```text
Source runtime (SQL Server):
  freetds:                ✓ installed + registered  /  ✗ not installed  /  ✗ unixODBC missing  /  ✗ not registered
  toolbox:                ✓ installed (x.y.z)  /  — not found (optional until ad-migration setup-source)
  mssql_driver_override:  ✓ resolved  /  — default FreeTDS  /  ✗ manual override needed
```

**For source = Oracle:**

```text
Source runtime (Oracle):
  sqlcl:               ✓ installed  /  ✗ not found
  java:                ✓ 11+ (x.y.z)  /  ✗ not found or < 11
  oracle_mcp:          ✓ starts  /  ✗ fails
  sqlcl_bin_override:  ✓ resolved  /  — PATH default  /  ✗ manual override needed
```

**For target = SQL Server:**

```text
Target runtime (SQL Server):
  pyodbc:                 ✓ importable  /  ✗ missing
  mssql_driver_override:  ✓ resolved  /  — default FreeTDS  /  ✗ manual override needed
```

**For target = Oracle:**

```text
Target runtime (Oracle):
  oracledb:  ✓ importable  /  ✗ missing
```

`toolbox` and `direnv` are marked `—` (not `✗`) when missing. `toolbox` is optional for DDL file mode but required later for SQL Server `ad-migration setup-source`.

For SQL Server, `freetds` is only green after both installation and unixODBC registration pass. If `brew` reports FreeTDS installed but `odbcinst` is missing, treat that as a failed prerequisite because `ad-migration setup-source` will not work with the default `MSSQL_DRIVER="FreeTDS"` path.

If any local override is discovered or required, explain that init will write only non-secret machine-specific overrides into `.env`, while `.envrc` remains the shared repo-local scaffold:

> **Recommended: keep machine-local overrides in `.env`.** The scaffolding step writes repo-shared environment scaffolding to `.envrc` and loads `.env` when present. Use `.env` only for non-secret local overrides such as `MSSQL_DRIVER` or `SQLCL_BIN`. Connection details are collected later in the stage-specific setup commands.

Do not ask the user for source, target, or sandbox connection details during init. Those belong to `ad-migration setup-source`, `ad-migration setup-target`, and `ad-migration setup-sandbox`.

If everything is already validated (all items `true` in the existing handoff and no new gaps found), say "All prerequisites validated" and proceed directly to Step 6 without asking for confirmation. Otherwise, ask the user to confirm before proceeding with Step 5.

## Step 5: Execute

Only after the user confirms, run the needed actions:

**Install uv** (if missing):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installing, re-run `uv --version` to confirm. Tell the user to restart their shell if the command is not found after install.

**Python missing**: cannot auto-install. Tell the user to install Python 3.11+ from `https://python.org/downloads` and re-run `/init-ad-migration` after installing. Stop here for this check — do not proceed to shared deps without Python.

**Sync shared deps** (if not synced):

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib"
```

**Install FreeTDS** (SQL Server, if missing):

```bash
brew install freetds
```

After installing, re-run `brew list --formula freetds` to confirm. FreeTDS is the default ODBC driver for SQL Server connectivity. Users who prefer the Microsoft driver can set `MSSQL_DRIVER="ODBC Driver 18 for SQL Server"` and install `msodbcsql18` themselves (requires interactive EULA acceptance).

**Register FreeTDS in unixODBC** (SQL Server, when installed but not registered):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init check-freetds --register-missing
```

This command must succeed and report `registered: true` before you record `freetds: true` in the handoff.

**Sync source-specific deps**:

For SQL Server:

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib" --extra export
```

For Oracle:

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib" --extra oracle
```

**Verify target client libraries**:

For SQL Server target:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import pyodbc"
```

For Oracle target:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import oracledb"
```

**ddl_mcp fails** (after shared sync): re-run the ddl_mcp check. If it still fails, show the error output to the user and tell them to check their Python environment.

**toolbox missing** (SQL Server, if the user asks how to install it): Direct the user to `https://github.com/googleapis/genai-toolbox/releases` to download the binary for their platform and add it to PATH. Do not attempt to install it automatically.

**SQLcl or Java missing** (Oracle): Direct the user to install SQLcl from `https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/` and ensure Java 11+ is on PATH. Do not attempt to install automatically.

**direnv missing** (if the user asks how to install it): Direct them to `https://direnv.net` for install instructions. Do not attempt to install it automatically.

## Step 6: Scaffold project files

Run the `init` CLI to scaffold the project directory, passing the chosen source technology. This creates CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, `scripts/worktree.sh`, `.claude/rules/git-workflow.md`, and `.githooks/pre-commit` — all idempotently and parameterized by source.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init scaffold-project --project-root . --technology $SOURCE
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init scaffold-hooks --project-root . --technology $SOURCE
```

Parse the JSON output and report to the user which files were created, updated, or skipped.

If `scaffold-project` reports missing CLAUDE.md sections (in `files_skipped`), tell the user which sections are missing and recommend adding them.

Maintain a JSON object `$OVERRIDES` while gathering evidence. Add only non-secret machine-specific resolved overrides such as `MSSQL_DRIVER` or `SQLCL_BIN`. Do not add connection details during init.

If `$OVERRIDES` is non-empty, write it to `.env` in the project root:

Use the deterministic init helper for that write:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init write-local-env-overrides --project-root . --overrides-json '$OVERRIDES'
```

Then write the partial manifest with prerequisite validation results. Build a JSON object `$PREREQS` from the combined results of Steps 3-5 (merging any existing handoff values with newly validated items). The object should record common startup checks plus role-scoped startup readiness:

```json
{
  "common": {
    "startup": {"uv": true, "python": true, "shared_deps": true, "ddl_mcp": true, "direnv": false}
  },
  "roles": {
    "source": {"technology": "sql_server", "startup": {"freetds": true, "toolbox": false, "driver_override_resolved": true}},
    "sandbox": {"technology": "sql_server", "startup": {"freetds": true, "driver_override_resolved": true}},
    "target": {"technology": "oracle", "startup": {"oracledb": true}}
  }
}
```

Pass it to the CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" setup-ddl write-partial-manifest --project-root . --technology $SOURCE --target-technology $TARGET --prereqs-json '$PREREQS'
```

Re-running `/init-ad-migration` reads the handoff from `manifest.json` and skips already-passing checks.

## Step 7: Commit

If the working directory is a git repository, check the current branch first:

```bash
git branch --show-current
```

If on `main`, notify the user:

> ⚠️ Committing init files directly to `main`. Init scaffolding is typically committed to main — this is expected. For migration work that follows, create a feature branch before running `/scope`, `/profile`, or other pipeline commands.

Commit the files created or modified in Step 5:

```bash
git add CLAUDE.md README.md .gitignore .githooks/ repo-map.json .claude/ scripts/ manifest.json
git commit -m "chore: init migration project ($SOURCE)"
```

Do not stage `.envrc` or `.env` — both are local environment files. `.envrc` contains repo-local environment scaffolding and `.env` contains machine-local overrides.

If not a git repository, skip silently.

Then tell the user: **"Restart Claude to pick up the new project instructions."**

## Step 8: Handoff

Tell the user:

**For source = SQL Server:**

- **toolbox installed and SOURCE_MSSQL_* vars set**: ready to run `ad-migration setup-source --technology sql_server --schemas <schema>` to extract DDL from the live database.
- **toolbox missing or SOURCE_MSSQL_* vars unset**: Set `SOURCE_MSSQL_HOST/PORT/DB/USER/PASSWORD` in `.envrc`, run `direnv allow`, install `toolbox`, then run `ad-migration setup-source`.

**For source = Oracle:**

- **SQLcl + Java installed and SOURCE_ORACLE_* vars set**: ready to run `ad-migration setup-source --technology oracle --schemas <schema>` to extract DDL from the live database.
- **SQLcl/Java missing or SOURCE_ORACLE_* vars unset**: Set `SOURCE_ORACLE_HOST/PORT/SERVICE/USER/PASSWORD` in `.envrc`, run `direnv allow`, ensure SQLcl and Java 11+ are installed, then run `ad-migration setup-source`.

**For target setup:**

- **target toolchain ready**: proceed to `/setup-target` when ready to collect target connection/runtime details.
- **target toolchain not ready**: install the missing client library or local override first, then run `/setup-target`.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `init scaffold-project` | non-zero | File IO failure. Surface error message, stop |
| `init scaffold-project` | 0 + `files_skipped` non-empty | Files already exist. Report which were skipped — not an error |
| `init scaffold-hooks` | non-zero | Hook creation or git config failed. Surface error message |
| `setup-ddl write-partial-manifest` | non-zero | Technology validation or IO failure. Surface error message, stop |
| `uv run ... python3 -c "import ..."` | non-zero | Shared deps not synced. Tell user to run `uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib"` |

## Idempotency

Safe to re-run. Each step checks current state before acting:

- Runtime selection re-evaluates `$ARGUMENTS` each time for the source role and re-prompts for target when needed.
- Step 3 reads existing `init_handoff` from `manifest.json` and skips checks already recorded as passing for the same selected technology. Only items marked `false`, missing, or tied to a different selected technology are re-checked.
- Step 5 uses the `init` CLI which is fully idempotent: existing CLAUDE.md is checked for missing sections (not overwritten), README.md and repo-map.json are skipped if present, .gitignore gets only missing entries appended, .envrc is skipped if present, .claude/rules/git-workflow.md is skipped if present, .githooks/pre-commit is skipped if present.
- Step 6 partial manifest updates `runtime.source`, `runtime.sandbox`, and `runtime.target`, merges existing handoff passes with newly validated items, and updates the timestamp.
- Step 7 only commits if there are staged changes.
