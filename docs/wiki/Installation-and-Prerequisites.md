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
| [direnv](https://direnv.net) | Recommended for all projects | Auto-loads `.envrc` credentials when you enter the project directory; keeps secrets out of shell history |

## Environment Variables

The variables required depend on your source technology. The `/init-ad-migration` command scaffolds a `.envrc` with only the variables for the selected source. These bootstrap the first live connection; the canonical runtime contract is then persisted in `manifest.json` under `runtime.source`, `runtime.target`, and `runtime.sandbox`.

### SQL Server

| Variable | Description | Example |
|---|---|---|
| `SOURCE_MSSQL_HOST` | SQL Server hostname or IP | `localhost` |
| `SOURCE_MSSQL_PORT` | SQL Server port | `1433` |
| `SOURCE_MSSQL_DB` | Configured source database that contains the runtime `MigrationTest` schema fixture | `AdventureWorks2022` |
| `SOURCE_MSSQL_USER` | SQL login username | `sa` |
| `SOURCE_MSSQL_PASSWORD` | SQL login password | _(from env)_ |
| `MSSQL_DRIVER` | _(optional)_ ODBC driver override | `FreeTDS` _(default)_ |

`MSSQL_DRIVER` defaults to `FreeTDS`. Set it to `ODBC Driver 18 for SQL Server` if you prefer the Microsoft driver (requires `brew install msodbcsql18` with interactive EULA acceptance).

When using the default `FreeTDS` path, `/init-ad-migration` now verifies both the Homebrew package and the unixODBC driver registration. A plain `brew install freetds` is not considered sufficient if `FreeTDS` does not appear in `odbcinst -q -d`.

All connection variables are required for `ad-migration setup-source`, `/setup-sandbox`, `/generate-tests`, `/refactor`, and any other live-database skill.

### Oracle

| Variable | Description | Example |
|---|---|---|
| `SOURCE_ORACLE_HOST` | Oracle hostname or IP | `localhost` |
| `SOURCE_ORACLE_PORT` | Oracle listener port | `1521` |
| `SOURCE_ORACLE_SERVICE` | Oracle service name | `FREEPDB1` |
| `SOURCE_ORACLE_USER` | Oracle username | `sh` |
| `SOURCE_ORACLE_PASSWORD` | Oracle password | _(from env)_ |

### Setting variables with direnv (recommended)

The `/init-ad-migration` command scaffolds a tracked `.envrc` template for shared non-secret variables and loads local secrets from `.env`. Fill in `.envrc`, put password values in `.env`, then run:

```bash
direnv allow
```

### Setting variables manually

Export them in your shell before launching `claude`:

```bash
# SQL Server
export SOURCE_MSSQL_HOST=localhost
export SOURCE_MSSQL_PORT=1433
export SOURCE_MSSQL_DB=AdventureWorks2022
export SOURCE_MSSQL_USER=sa
export SOURCE_MSSQL_PASSWORD=<your-password>

# Oracle
export SOURCE_ORACLE_HOST=localhost
export SOURCE_ORACLE_PORT=1521
export SOURCE_ORACLE_SERVICE=FREEPDB1
export SOURCE_ORACLE_USER=sh
export SOURCE_ORACLE_PASSWORD=<your-password>
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

## Installing the ad-migration CLI

The `ad-migration` CLI is installed automatically when you run `/init-ad-migration`. To install
manually or verify an existing installation:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
ad-migration --version
```

Dev usage without installing:

```bash
uv run --project lib ad-migration --help
```

## Verifying Setup

Run the initialization command inside your Claude Code session:

```text
/init-ad-migration
```

This installs the `ad-migration` CLI via Homebrew if not already present, then prompts for source technology selection, checks every prerequisite silently, and presents a status display grouped by source. See [[Stage 1 Project Init]] for full details on the status display format and what each check covers.

## Next Step

Once verification passes, proceed to the [[Quickstart]] for a complete walkthrough.
