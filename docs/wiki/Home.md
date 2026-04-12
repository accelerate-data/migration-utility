# Migration Utility

A Claude Code plugin and batch CLI pipeline that migrates Microsoft Fabric Warehouse stored procedures to dbt models. It targets silver and gold transformations; bronze remains out of scope.

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
| 1 | `/init-ad-migration` | Scaffolds project files, git hooks, and `scripts/worktree.sh` |
| 2 | `/setup-ddl` | Extracts DDL and builds the local catalog |
| 3 | `/init-dbt` | Scaffolds the dbt project and generates `sources.yml` |
| 4 | `/setup-sandbox` | Creates the throwaway database used for proof-backed testing |

### Per-object migration

```text
/scope
  -> /profile
  -> /generate-tests
  -> /refactor
  -> /generate-model
```

Batch commands create or reuse worktrees through `git-checkpoints`, commit successful items as they finish, and can raise a PR for the run at the end.

## Interactive vs batch

| Mode | Entry point | Best for |
|---|---|---|
| Interactive | Skills such as `/listing-objects`, `/analyzing-table`, `/profiling-table` | Exploring or fixing one object at a time |
| Batch | Commands such as `/scope`, `/profile`, `/generate-tests`, `/refactor`, `/generate-model` | Processing multiple objects with parallel sub-agents and git automation |

## User-invocable commands

The plugin currently exposes these user-facing commands:

- `/init-ad-migration`
- `/setup-ddl`
- `/init-dbt`
- `/setup-sandbox`
- `/scope`
- `/profile`
- `/generate-tests`
- `/refactor`
- `/generate-model`
- `/status`
- `/add-source-tables`
- `/exclude-table`
- `/reset-migration`
- `/commit`
- `/commit-push-pr`
- `/teardown-sandbox`
- `/cleanup-worktrees`

See [[Command Reference]] for a one-page summary.

## User-invocable skills

The main user-facing skills are:

- `/listing-objects`
- `/analyzing-table`
- `/profiling-table`
- `/generating-tests`
- `/generating-model`

Internal skills such as `git-checkpoints`, `reviewing-tests`, `reviewing-model`, and `test-invariants` support the batch commands but are not the normal user entrypoints.

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
scripts/worktree.sh
```

The catalog is the durable project state. Batch commands read from it, write back to it, and persist successful outputs to git.
