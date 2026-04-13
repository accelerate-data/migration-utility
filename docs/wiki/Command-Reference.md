# Command Reference

This page lists the current user-invocable commands exposed by the plugin.

## Project setup

| Command | Purpose |
|---|---|
| `/init-ad-migration` | Scaffold the migration repo and check prerequisites |
| `/setup-ddl` | Extract DDL and build the local catalog |
| `/setup-target` | Configure the target runtime, scaffold the dbt project, and generate `sources.yml` |
| `/setup-sandbox` | Create the active sandbox execution endpoint |
| `/teardown-sandbox` | Drop the sandbox endpoint and clear sandbox metadata |

## Migration pipeline

| Command | Purpose |
|---|---|
| `/scope` | Resolve writers for tables or analyze views |
| `/profile` | Write migration profiles for tables, views, or MVs |
| `/generate-tests` | Generate and review test scenarios, then capture ground truth |
| `/refactor` | Persist proof-backed import/logical/final refactors |
| `/generate-model` | Generate dbt artifacts from approved refactors and tests |
| `/status` | Show current readiness and the next best action |

## Source and scope management

| Command | Purpose |
|---|---|
| `/add-source-tables` | Confirm tables as dbt sources (`is_source: true`) |
| `/exclude-table` | Exclude tables or views from the active migration pipeline |
| `/reset-migration` | Clear one migration stage so it can be re-run cleanly |

## Git and workflow helpers

| Command | Purpose |
|---|---|
| `/commit` | Stage specific files, commit, and push |
| `/commit-push-pr` | Stage specific files, commit, push, and open or update a PR |
| `/cleanup-worktrees` | Remove merged worktrees and stale merged branches |

## Notes

- Batch commands use the git checkpoint flow and may create or reuse worktrees.
- Successful items are usually committed as they complete, not held for a single end-of-batch approval step.
- Source-confirmed tables are skipped by downstream migration commands because they are not migration targets.
