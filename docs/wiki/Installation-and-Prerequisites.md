# Installation and Prerequisites

This page covers the tools, environment variables, and verification steps needed before running the migration utility.

## Required Tools

| Tool | Version | Purpose |
|---|---|---|
| [Claude Code CLI](https://claude.ai) | Latest | Agent runtime that executes all plugin commands |
| [uv](https://github.com/astral-sh/uv) | Latest | Python package manager and runner used by every CLI tool |
| Python | 3.11+ | Runtime for shared library, MCP server, and CLI tools |
| [gh CLI](https://cli.github.com/) | Latest | GitHub operations (PRs, branch management, worktree cleanup) |
| git | 2.x+ | Version control; worktree support required for batch commands |

## Optional Tools

| Tool | When needed | Purpose |
|---|---|---|
| [genai-toolbox](https://github.com/googleapis/genai-toolbox/releases) (`toolbox`) | Live DB extraction via `/setup-ddl` | HTTP-mode MCP server that bridges Claude Code to SQL Server |
| [direnv](https://direnv.net) | Recommended for all projects | Auto-loads `.envrc` credentials when you enter the project directory; keeps secrets out of shell history |

## Environment Variables

These four variables configure SQL Server connectivity. They are read by the `mssql` MCP server at startup via environment inheritance, so they must be set **before** launching `claude`.

| Variable | Description | Example |
|---|---|---|
| `MSSQL_HOST` | SQL Server hostname or IP | `localhost` |
| `MSSQL_PORT` | SQL Server port | `1433` |
| `MSSQL_DB` | Default database (use `master` if no specific default) | `AdventureWorksDW` |
| `SA_PASSWORD` | SQL login password | _(from env)_ |

All four are required for `/setup-ddl`, `/setup-sandbox`, `/generate-tests`, and any other live-database skill. They are not needed for DDL-file-import mode.

### Setting variables with direnv (recommended)

The `/init-ad-migration` command scaffolds a `.envrc` template. Fill in your values and run:

```bash
direnv allow
```

### Setting variables manually

Export them in your shell before launching `claude`:

```bash
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=AdventureWorksDW
export SA_PASSWORD=<your-password>
```

## Loading the Plugin

Launch Claude Code with the ad-migration plugin directory:

```bash
claude --plugin-dir <path-to-ad-migration>
```

The plugin directory contains the bootstrap, migration, and ground-truth-harness plugins that provide all pipeline commands.

## Verifying Setup

Run the initialization command inside your Claude Code session:

```text
/init-ad-migration
```

This checks every prerequisite silently and presents a status display:

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
```

Missing required tools trigger an installation flow. Missing optional tools (`toolbox`, `direnv`) and unset MSSQL variables are flagged but do not block setup of the core tools.

## Next Step

Once verification passes, proceed to the [[Quickstart]] for a complete walkthrough.
