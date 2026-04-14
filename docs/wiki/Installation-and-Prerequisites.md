# Installation and Prerequisites

This page covers the tools, environment variables, and verification steps needed before running the migration utility.

## Platform Support

Local execution is supported on macOS and Linux. Windows is not supported for the current local workflow because the project setup depends on Unix-oriented tooling such as `brew`, `direnv`, bash hooks, and unixODBC-based FreeTDS registration.

## Required Tools

| Tool | Version | Purpose |
|---|---|---|
| [Claude Code CLI](https://claude.ai) | Latest | Agent runtime that executes all plugin commands |
| [uv](https://github.com/astral-sh/uv) | Latest | Python package manager and runner used by every CLI tool |
| Python | 3.11+ | Runtime for shared library, MCP server, and CLI tools |
| [gh CLI](https://cli.github.com/) | Latest | GitHub operations (PRs, branch management, worktree cleanup) |
| git | 2.x+ | Version control; worktree support required for batch commands |

## Required Tools (SQL Server)

| Tool | Version | Purpose |
|---|---|---|
| [FreeTDS](https://www.freetds.org/) | Latest | Open-source ODBC driver for SQL Server connectivity. Install: `brew install freetds` and ensure it is registered in unixODBC |

## Optional Tools

| Tool | When needed | Purpose |
|---|---|---|
| [genai-toolbox](https://github.com/googleapis/genai-toolbox/releases) (`toolbox`) | Live SQL Server extraction via `/setup-ddl` | HTTP-mode MCP server that bridges Claude Code to SQL Server |
| [SQLcl](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/) (`sql`) | Oracle source projects | CLI tool that provides the Oracle MCP server via `sql -mcp`; requires Java 11+. `sql` must be on PATH — the plugin invokes it directly. If another `sql` binary conflicts, override `command` in your local `.mcp.json` with the absolute path to the SQLcl binary. |
| Java 11+ | Oracle source projects | Runtime required by SQLcl |
| [direnv](https://direnv.net) | Recommended for all projects | Auto-loads `.envrc` credentials when you enter the project directory; keeps secrets out of shell history |

If you want `claude --plugin-dir .` to load every bundled MCP server locally,
`toolbox` and `sql` must already be on `PATH`. Without them, the plugin manifest
still validates, but the corresponding `mssql` or `oracle` server will not
start.

## Environment Variables

The variables required depend on your source technology. The `/init-ad-migration` command scaffolds a `.envrc` with only the variables for the selected source. These bootstrap the first live connection; the canonical runtime contract is then persisted in `manifest.json` under `runtime.source`, `runtime.target`, and `runtime.sandbox`.

### SQL Server

| Variable | Description | Example |
|---|---|---|
| `MSSQL_HOST` | SQL Server hostname or IP | `localhost` |
| `MSSQL_PORT` | SQL Server port | `1433` |
| `MSSQL_DB` | Configured source database that contains the runtime `MigrationTest` schema fixture | `AdventureWorks2022` |
| `SA_PASSWORD` | SQL login password | _(from env)_ |
| `MSSQL_DRIVER` | _(optional)_ ODBC driver override | `FreeTDS` _(default)_ |

`MSSQL_DRIVER` defaults to `FreeTDS`. Set it to `ODBC Driver 18 for SQL Server` if you prefer the Microsoft driver (requires `brew install msodbcsql18` with interactive EULA acceptance).

When using the default `FreeTDS` path, `/init-ad-migration` now verifies both the Homebrew package and the unixODBC driver registration. A plain `brew install freetds` is not considered sufficient if `FreeTDS` does not appear in `odbcinst -q -d`.

All four connection variables are required for `/setup-ddl`, `/setup-sandbox`, `/generate-tests`, `/refactor`, and any other live-database skill.

### Oracle

| Variable | Description | Example |
|---|---|---|
| `ORACLE_HOST` | Oracle hostname or IP | `localhost` |
| `ORACLE_PORT` | Oracle listener port | `1521` |
| `ORACLE_SERVICE` | Oracle service name | `FREEPDB1` |
| `ORACLE_USER` | Oracle username | `sh` |
| `ORACLE_PASSWORD` | Oracle password | _(from env)_ |

### Setting variables with direnv (recommended)

The `/init-ad-migration` command scaffolds a `.envrc` template containing only the variables for your selected source. Fill in your values and run:

```bash
direnv allow
```

### Setting variables manually

Export them in your shell before launching `claude`:

```bash
# SQL Server
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=AdventureWorks2022
export SA_PASSWORD=<your-password>

# Oracle
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_SERVICE=FREEPDB1
export ORACLE_USER=sh
export ORACLE_PASSWORD=<your-password>
```

## Python Dependencies

The shared library depends on pydantic, sqlglot, and typer as its key runtime libraries. All Python dependencies (including transitive ones) are managed by uv and pinned in `lib/pyproject.toml`. Running `uv sync` in the `lib` directory installs everything needed.

## Loading the Plugin

Install from the Vibedata marketplace:

```bash
/plugin marketplace add accelerate-data/vibedata-plugins-official
/plugin install ad-migration@vibedata-plugins-official
```

Alternatively, for local development, load the plugin directly:

```bash
claude --plugin-dir .
```

The `ad-migration` plugin provides all pipeline commands and skills in a single package.

## Verifying Setup

Run the initialization command inside your Claude Code session:

```text
/init-ad-migration
```

This prompts for source technology selection, then checks every prerequisite silently and presents a status display grouped by source. See [[Stage 1 Project Init]] for full details on the status display format and what each check covers.

## Next Step

Once verification passes, proceed to the [[Quickstart]] for a complete walkthrough.
