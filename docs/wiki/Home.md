# Migration Utility

A Claude Code plugin and batch CLI pipeline that migrates warehouse stored procedures to dbt models. It targets silver and gold transformations; bronze remains out of scope.

## Who uses it

- Field Data Engineers running customer migrations
- Customers doing self-service stored-procedure-to-dbt conversions

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
/scope
  -> /profile
  -> /generate-tests
  -> /refactor
  -> /generate-model
```

Batch commands create or reuse worktrees through `git-checkpoints` and manage their own batch git workflow. The `ad-migration` CLI does not commit, push, open PRs, or clean worktrees for you.

## Interactive vs batch

| Mode | Entry point | Best for |
|---|---|---|
| Interactive | Skills such as `/listing-objects`, `/analyzing-table`, `/profiling-table` | Exploring or fixing one object at a time |
| Batch | Commands such as `/scope`, `/profile`, `/generate-tests`, `/refactor`, `/generate-model` | Processing multiple objects with parallel sub-agents and git automation |

## User-invocable commands

The plugin currently exposes these user-facing commands:

- `/init-ad-migration`
- `/scope`
- `/profile`
- `/generate-tests`
- `/refactor`
- `/generate-model`
- `/status`
- `/cleanup-worktrees`

See [[Command Reference]] for a one-page summary.

The user-facing CLI commands are:

- `ad-migration setup-source`
- `ad-migration setup-target`
- `ad-migration setup-sandbox`
- `ad-migration teardown-sandbox`
- `ad-migration reset`
- `ad-migration exclude-table`
- `ad-migration add-source-table`

## User-invocable skills

The main user-facing skills are:

- `/listing-objects`
- `/analyzing-table`
- `/profiling-table`

Internal skills such as `generating-tests`, `generating-model`, `git-checkpoints`, `reviewing-tests`, `reviewing-model`, `refactoring-sql`, and `test-invariants` support the batch commands but are not user entrypoints.

## Where to start

- New repo: [[Installation and Prerequisites]] then [[Quickstart]]
- Specific stage: use the sidebar stage pages
- Troubleshooting: [[Troubleshooting and Error Codes]]

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
