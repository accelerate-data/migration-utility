---
name: init-ad-migration
description: Checks prerequisites for the chosen source technology, installs missing deps, scaffolds project files (CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .githooks), writes a partial manifest, and hands off to /setup-ddl.
user-invocable: true
---

# Initialize ad-migration plugin

Verify and set up all prerequisites before using `listing-objects`, `analyzing-table`, or `/setup-ddl`. Then scaffold the project directory for both agents and human developers.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop immediately and tell the user to load the plugin with `claude --plugin-dir <path-to-ad-migration>` before running this command.

## Step 2: Source selection

Determine which source technology to configure:

1. Check `$ARGUMENTS` for a positional source slug (e.g. `/init-ad-migration oracle` or `/init-ad-migration sql_server`).
2. If no argument was provided, ask the user to choose:

> **Which source database are you migrating from?**
>
> 1. `sql_server` — Microsoft SQL Server (T-SQL)
> 2. `oracle` — Oracle Database (PL/SQL)

Validate the chosen slug against the source registry in `init.py`. If the slug is unknown, list the valid options and ask again.

Store the chosen slug as `$SOURCE` for the remaining steps.

## Step 3: Gather evidence

Run all checks **silently** — do NOT install or change anything yet.

### Common prerequisites (all sources)

1. `uv --version` — is uv installed?
2. `python3 --version` — is Python >= 3.11?
3. `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" python3 -c "import pydantic, sqlglot, typer"` — are shared package deps synced?
4. `uv run "${CLAUDE_PLUGIN_ROOT}/mcp/ddl/server.py" --help` — does the DDL MCP server start cleanly?
5. `git rev-parse --is-inside-work-tree` — is the current working directory inside a git repository? If not, warn the user that the project folder is not under version control and recommend initialising git before running extraction skills.
6. `direnv version` — is direnv installed? This is optional; mark as `—` if missing.

### SQL Server prerequisites (when `$SOURCE` is `sql_server`)

1. `toolbox --version` — is the genai-toolbox binary installed?
2. Check whether each of the four MSSQL environment variables is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.
3. If all MSSQL env vars are set, verify the MCP server: `uv run "${CLAUDE_PLUGIN_ROOT}/mcp/ddl/server.py" --help`

### Oracle prerequisites (when `$SOURCE` is `oracle`)

1. `sql -V` — is SQLcl installed?
2. `java -version` — is Java 11+ installed?
3. Check whether each of the five Oracle environment variables is set (non-empty): `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`. Do not print their values.
4. If all Oracle env vars are set, verify the Oracle MCP server can start: test that `sql -mcp` exits cleanly (does NOT require a live DB connection — just checks the binary runs).

## Step 4: Present plan

Show the user what was found and what needs to be done, grouped into common and source-specific sections:

```text
Common prerequisites:
  uv:          ✓ installed (x.y.z)  /  ✗ not found
  python:      ✓ 3.x.x              /  ✗ not found or < 3.11
  shared deps: ✓ synced             /  ✗ not synced
  ddl_mcp:     ✓ starts             /  ✗ fails
  git:         ✓ repo detected      /  — not a git repo (recommended)
  direnv:      ✓ installed (x.y.z)  /  — not found (recommended)
```

**For SQL Server:**

```text
SQL Server prerequisites:
  toolbox:     ✓ installed (x.y.z)  /  — not found (optional)
  MSSQL_HOST:  ✓ set  /  — not set
  MSSQL_PORT:  ✓ set  /  — not set
  MSSQL_DB:    ✓ set  /  — not set
  SA_PASSWORD: ✓ set  /  — not set
```

**For Oracle:**

```text
Oracle prerequisites:
  sqlcl:          ✓ installed          /  ✗ not found
  java:           ✓ 11+ (x.y.z)       /  ✗ not found or < 11
  oracle_mcp:     ✓ starts             /  ✗ fails  /  — skipped (env vars missing)
  ORACLE_HOST:    ✓ set  /  — not set
  ORACLE_PORT:    ✓ set  /  — not set
  ORACLE_SERVICE: ✓ set  /  — not set
  ORACLE_USER:    ✓ set  /  — not set
  ORACLE_PASSWORD: ✓ set  /  — not set
```

`toolbox`, `direnv`, and the source credentials are marked `—` (not `✗`) when missing — they are optional for DDL file mode but required for `/setup-ddl` and any live-database skill. They will not block setup of the core tools.

If any credential variable is unset, recommend using direnv for credential management:

> **Recommended: use direnv for credentials.** The scaffolding step will create a `.envrc` template with the correct variables for your source technology. Fill in your values and run `direnv allow`. This keeps credentials out of your shell history and loads them automatically when you enter the project directory.
>
> If you prefer not to use direnv, export the variables in your shell before launching `claude`.

These values are passed to the MCP server at startup via environment inheritance — they must be set before launching `claude`, not after.

If everything is already set up, say so and skip Step 5 (nothing to install). Proceed directly to Step 6. Otherwise, ask the user to confirm before proceeding.

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

**Sync source-specific deps**:

For SQL Server:

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib" --extra export
```

For Oracle:

```bash
uv sync --project "${CLAUDE_PLUGIN_ROOT}/lib" --extra oracle
```

**ddl_mcp fails** (after shared sync): re-run the ddl_mcp check. If it still fails, show the error output to the user and tell them to check their Python environment.

**toolbox missing** (SQL Server, if the user asks how to install it): Direct the user to `https://github.com/googleapis/genai-toolbox/releases` to download the binary for their platform and add it to PATH. Do not attempt to install it automatically.

**SQLcl or Java missing** (Oracle): Direct the user to install SQLcl from `https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/` and ensure Java 11+ is on PATH. Do not attempt to install automatically.

**direnv missing** (if the user asks how to install it): Direct them to `https://direnv.net` for install instructions. Do not attempt to install it automatically.

## Step 6: Scaffold project files

Run the `init` CLI to scaffold the project directory, passing the chosen technology. This creates CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .claude/rules/git-workflow.md, and .githooks/pre-commit — all idempotently and parameterized by source.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init scaffold-project --project-root . --technology $SOURCE
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" init scaffold-hooks --project-root . --technology $SOURCE
```

Parse the JSON output and report to the user which files were created, updated, or skipped.

If `scaffold-project` reports missing CLAUDE.md sections (in `files_skipped`), tell the user which sections are missing and recommend adding them.

Then write the partial manifest:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" setup-ddl write-partial-manifest --project-root . --technology $SOURCE
```

This creates `manifest.json` with the chosen technology and dialect. `/setup-ddl` will later enrich it with database and schema details.

## Step 7: Commit

If the working directory is a git repository, check the current branch first:

```bash
git branch --show-current
```

If on `main`, notify the user:

> ⚠️ Committing init files directly to `main`. Init scaffolding is typically committed to main — this is expected. For migration work that follows, create a feature branch before running `/scope`, `/profile`, or other pipeline commands.

Commit the files created or modified in Step 5:

```bash
git add CLAUDE.md README.md .gitignore .githooks/ repo-map.json .claude/ manifest.json
git commit -m "chore: init migration project ($SOURCE)"
```

Do not stage `.envrc` — it is gitignored and contains credentials.

If not a git repository, skip silently.

Then tell the user: **"Restart Claude to pick up the new project instructions."**

## Step 8: Handoff

Tell the user:

**For SQL Server:**

- **toolbox installed and all MSSQL vars set**: ready to run `/setup-ddl` to extract DDL from the live database.
- **toolbox missing or MSSQL vars unset**: DDL file mode (`listing-objects`, `analyzing-table`, `scoping`) is fully available. Live-database skills (`/setup-ddl`) require both `toolbox` and all four MSSQL env vars. If using direnv, fill in `.envrc` and run `direnv allow`. Then install `toolbox` from the genai-toolbox releases page.

**For Oracle:**

- **SQLcl + Java installed and all Oracle vars set**: ready to run `/setup-ddl` to extract DDL from the live database. Remember: the Oracle MCP server requires a manual connect step at the start of each session.
- **SQLcl/Java missing or Oracle vars unset**: DDL file mode (`listing-objects`, `analyzing-table`, `scoping`) is fully available. Live-database skills (`/setup-ddl`) require SQLcl, Java 11+, and all five Oracle env vars. If using direnv, fill in `.envrc` and run `direnv allow`.

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

- Source selection re-evaluates `$ARGUMENTS` each time.
- Checks in Step 2 re-evaluate actual environment state.
- Step 5 uses the `init` CLI which is fully idempotent: existing CLAUDE.md is checked for missing sections (not overwritten), README.md and repo-map.json are skipped if present, .gitignore gets only missing entries appended, .envrc is skipped if present, .claude/rules/git-workflow.md is skipped if present, .githooks/pre-commit is skipped if present.
- Step 5 partial manifest overwrites with the same technology/dialect (safe).
- Step 6 only commits if there are staged changes.
