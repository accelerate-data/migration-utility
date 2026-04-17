# Installation and Prerequisites

This page covers the tools, installation steps, and verification flow needed before running the migration utility.

## What is Claude Code?

[Claude Code](https://docs.anthropic.com/en/docs/claude-code) is an AI-powered CLI that runs in your terminal. You interact with it by typing natural language or `/` commands in a chat-style interface. The migration utility is a **plugin** that adds migration-specific commands (like `/scope-tables`, `/profile-tables`, `/generate-model`) to Claude Code. When you type a `/` command, the agent reads your project files, runs tools, and writes results. Interactive workflows pause for review; batch commands summarize results and ask before opening a PR.

## Platform Support

Local execution is supported on macOS, Linux, and WSL. Native Windows is not supported for the current local workflow; Use WSL for the local workflow.

## Required Tools

| Tool | Version | Purpose |
|---|---|---|
| [Claude Code CLI](https://claude.ai) | Latest | Agent runtime that executes all plugin commands |
| Python | 3.11+ | Runtime for CLI tools; installed automatically by the macOS Homebrew formula |
| [gh CLI](https://cli.github.com/) | Latest | GitHub operations (PRs, branch management, worktree cleanup) |
| git | 2.x+ | Version control; worktree support required for batch commands |

## Required Tools (SQL Server)

| Tool | Version | Purpose |
|---|---|---|
| [FreeTDS](https://www.freetds.org/) | Latest | Open-source ODBC driver for SQL Server connectivity. On macOS, install with `brew install freetds`. On Linux and WSL, install FreeTDS and unixODBC with your platform package manager. Ensure `FreeTDS` is registered in unixODBC. |

## Optional Tools

| Tool | When needed | Purpose |
|---|---|---|
| [direnv](https://direnv.net) | Recommended for all projects | Auto-loads `.envrc` credentials when you enter the project directory; keeps secrets out of shell history |

## Connection Variables

`/init-ad-migration` scaffolds a `.envrc` with the non-secret variables for the selected technology. The repo then uses role-specific variables for `runtime.source`, `runtime.target`, and `runtime.sandbox`.

Use the dedicated reference pages for the full variable lists:

- [[SQL Server Connection Variables]]
- [[Oracle Connection Variables]]

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

Add target and sandbox variables from the corresponding technology reference page before running `ad-migration setup-target` or `ad-migration setup-sandbox`.

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

The `ad-migration` CLI is installed automatically on macOS when you run `/init-ad-migration`. To install manually or verify an existing macOS installation:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
ad-migration --version
```

On Linux and WSL, install the supported Linux/WSL CLI package for your environment, then verify:

```bash
ad-migration --version
```

## Verifying Setup

Run the initialization command inside your Claude Code session:

```text
/init-ad-migration
```

On macOS, this installs the `ad-migration` CLI via Homebrew if not already present. On Linux and WSL, it reports the supported install path if the CLI is missing. It then prompts for source technology selection, checks every prerequisite silently, and presents a status display grouped by source. See [[Project Init]] for full details on the status display format and what each check covers.

## Next Step

Once verification passes, proceed to the [[Quickstart]] for a complete walkthrough.
