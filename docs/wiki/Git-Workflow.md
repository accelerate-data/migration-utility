# Git Workflow

The migration utility uses [git worktrees](https://git-scm.com/docs/git-worktree), structured branch names, and checkpoint commits to keep batch work isolated and reviewable. A worktree is a second working copy of the repo that shares the same `.git` directory.

## Worktrees

Batch stage commands (`/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model`) create or attach an isolated worktree before they write. `/migrate-mart-plan` creates a planning worktree and opens the planning PR. `/migrate-mart` creates a coordinator worktree, then launches one stage worktree at a time from the coordinator branch.

Worktrees are created at `../worktrees/<branchName>` relative to the repo root. For feature branches, the full prefix is preserved:

```text
../worktrees/feature/vu-354-scaffold-tauri-app
```

When a batch command creates a worktree, the branch name starts with `feature/` and uses the command's run slug:

```text
../worktrees/feature/scope-customer-dims
```

Created worktrees are bootstrapped with environment files and dependencies so they are ready to run immediately.

## Main-branch check

Batch commands do not write migration output directly on the default branch. They create or attach the planned worktree and report its branch, path, and PR in the command handoff.

## Branch naming

| Mode | Branch pattern | Created by |
|---|---|---|
| Interactive workflow | Your current branch | You manage manually |
| Single-item stage command | `feature/<command>-<schema>-<name>` | Command |
| Multi-item stage command | `feature/<short-run-slug>` | Command |
| Whole-mart planning and coordinator | `feature/migrate-mart-<slug>` | `/migrate-mart-plan`, then `/migrate-mart` |
| Whole-mart stage | `feature/migrate-mart-<slug>/<stage-id>-<stage-name>-<slug>` | `/migrate-mart` |

Single-object interactive workflows (`/analyzing-table`, `/profiling-table`, etc.) do not create branches. You work on whatever branch you are already on.

Batch commands generate deterministic run slugs for single objects and short descriptive run slugs for multi-object batches. Whole-mart commands use the plan slug so a rerun can attach to the same coordinator and stage worktrees.

## Commit granularity

Each item is committed as soon as it reaches a persisted non-error state in the loop. Items with status `error` are reverted inline before processing continues.

Checkpoint commits are pushed to remote immediately after committing.

### Commit message format

```text
<command>(<schema>.<table>): <one-line summary>
```

Examples:

```text
scope-tables(silver.DimCustomer): resolve usp_load_dimcustomer as selected writer
profile-tables(silver.DimProduct): classify as dim_scd1 with surrogate key
refactor-query(silver.DimCustomer): 4 CTEs, audit passed
generate-model(silver.FactSales): stg_factsales + mart, incremental materialization
```

## PR format

Pull requests have titles derived from the command and tables processed:

- **Title:** `<command>: <table1>, <table2>` (e.g. `scope-tables: silver.DimCustomer, silver.DimProduct`)
- **Body:** created by the command; batch summaries remain in the command output and run logs

Stage commands open or update their PR automatically and report the PR URL in the handoff. `/migrate-mart` merges each completed stage PR into the coordinator branch before moving to the next incomplete stage.

Standalone stage-command PRs target the repo's default branch for human review. Whole-mart stage PRs target the coordinator branch and are merged by `/migrate-mart`; the final coordinator PR targets the default branch and remains human-reviewed.

## Interactive vs multi-table differences

| Aspect | Interactive workflow | Batch command |
|---|---|---|
| Branching | user's current branch | Command-managed worktree |
| Approval | Reviewed inline | Per-item commits and PR handoff |
| PR creation | user manages manually | Command opens or updates PR |
| Error handling | user handles directly | Inline revert per error, surface in summary |

## Committing and pushing

Batch commands handle their own checkpoint commits and pushes as part of the command flow. For manual git work outside those commands, use normal git commands in the shell:

```bash
git add <files>
git commit -m "<message>"
git push
```

## What gets committed vs what stays local

| Committed (durable) | Never committed (ephemeral) |
|---|---|
| `catalog/tables/*.json` | `.migration-runs/` (`.gitignore`d) |
| `catalog/procedures/*.json` | |
| `test-specs/*.json` | |
| `dbt/models/**/*.sql` | |
| `dbt/models/**/*.yml` | |
| `ddl/*.sql` (from setup, not per-batch) | |

The `.migration-runs/` directory contains per-command execution metadata such as per-item status. Each file includes a run ID suffix so runs accumulate without overwriting. It is never committed.

## Cleaning up worktrees

After a PR is merged, the worktree and its branches are no longer needed. Run `/cleanup-worktrees` to remove them.

## Related pages

- [[Sandbox Operations]] -- sandbox lifecycle commands
- [[Status Dashboard]] -- checking pipeline progress
