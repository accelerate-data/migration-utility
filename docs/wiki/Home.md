# Migration Utility

A plugin for [Claude Code](https://docs.anthropic.com/en/docs/claude-code) that migrates warehouse stored procedures to dbt models. It targets silver and gold transformations; bronze remains out of scope.

## How it works

You work inside **Claude Code**, an AI-powered CLI that runs in your terminal. You type natural language or `/` commands in a chat-style interface. The plugin adds migration-specific commands (like `/scope-tables` and `/profile-tables`) that the agent executes on your behalf -- reading your catalog, analyzing SQL, writing files, and committing results to git.

There are also terminal CLI commands (like `ad-migration setup-source`) that you run directly in your shell, outside the Claude Code session. These handle deterministic setup tasks that don't need AI reasoning.

## Who uses it

- Data engineers running warehouse-to-dbt migrations
- Teams doing self-service stored-procedure-to-dbt conversions

## Pipeline overview

The workflow has two layers:

1. **Project setup** runs once per migration repo.
2. **Per-object migration** runs for each table or view you want to migrate.

### Project setup

| Step | Command | Result |
|---|---|---|
| 1 | `/init-ad-migration` | Scaffolds project files, git hooks, and repo-local workflow guidance |
| 2 | `ad-migration setup-source` | Extracts DDL and builds the local catalog |
| 3 | `ad-migration setup-target` | Collects target runtime, scaffolds the dbt project, and generates `sources.yml` |
| 4 | `ad-migration setup-sandbox` | Creates the throwaway database used for proof-backed testing |

### Per-object migration

```text
/scope-tables
  -> /profile-tables
  -> /generate-tests
  -> /refactor-query
  -> /generate-model
```

Batch commands create git worktrees to isolate their work and manage their own batch git workflow. The `ad-migration` CLI does not commit, push, open PRs, or clean worktrees for you.

## Interactive vs batch

| Mode | Entry point | Best for |
|---|---|---|
| Interactive | `/listing-objects`, `/analyzing-table`, `/profiling-table` | Exploring or fixing one object at a time |
| Batch | `/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model` | Processing multiple objects with git automation |

Both are typed as `/` commands inside a Claude Code session. Interactive commands work on a single object and wait for your input at decision points. Batch commands process a list of objects, commit each one as it finishes, and can open a PR at the end.

## Commands

The plugin exposes these `/` commands inside Claude Code:

- `/init-ad-migration`
- `/scope-tables`
- `/profile-tables`
- `/generate-tests`
- `/refactor-query`
- `/generate-model`
- `/status`
- `/cleanup-worktrees`

See [[Command Reference]] for a one-page summary.

The following CLI commands run directly in your terminal (not inside Claude Code):

- `ad-migration setup-source`
- `ad-migration setup-target`
- `ad-migration setup-sandbox`
- `ad-migration teardown-sandbox`
- `ad-migration reset`
- `ad-migration exclude-table`
- `ad-migration add-source-table`

The interactive commands (`/listing-objects`, `/analyzing-table`, `/profiling-table`) are useful for exploring one object at a time or debugging a specific table's catalog state.

## Where to start

- New repo: [[Installation and Prerequisites]] then [[Quickstart]]
- Specific stage: use the sidebar stage pages
- Troubleshooting: [[Troubleshooting and Error Codes]]
- Connection setup: [[SQL Server Connection Variables]] or [[Oracle Connection Variables]]

## Repository layout

A migration project produces a structure like:

```text
manifest.json
catalog/
  tables/<schema>.<table>.json
  procedures/<schema>.<proc>.json
  views/<schema>.<view>.json
  functions/<schema>.<func>.json
ddl/
test-specs/
dbt/
```

The catalog is the durable project state. Batch commands read from it, write back to it, and persist successful outputs to git.
