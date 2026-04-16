# Git Workflow

The migration utility uses [git worktrees](https://git-scm.com/docs/git-worktree), structured branch names, and per-item commits to keep batch work isolated and reviewable when you choose a feature branch. A worktree is a second working copy of the repo that shares the same `.git` directory.

## Worktrees

Batch commands (`/scope-tables`, `/profile-tables`, `/generate-tests`, `/refactor-query`, `/generate-model`) check your current branch before they write. If you are on the default branch, they ask whether to continue in place or create a feature branch with a worktree. If you are already on a feature branch, they use the current checkout.

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

Every batch command checks the current branch at startup. If you are on the default branch, the command asks whether to continue there or create a feature-branch worktree first.

If you continue on the default branch, the command runs in the current checkout. If you create a feature branch worktree, subsequent work in that run uses the new worktree path.

## Branch naming

| Mode | Branch pattern | Created by |
|---|---|---|
| Interactive workflow | Your current branch | You manage manually |
| Single-item batch command | `feature/<command>-<schema>-<name>` if you choose a worktree | Command, after confirmation |
| Multi-item batch command | `feature/<short-run-slug>` if you choose a worktree | Command, after confirmation |

Single-object interactive workflows (`/analyzing-table`, `/profiling-table`, etc.) do not create branches. You work on whatever branch you are already on.

Batch commands generate deterministic run slugs for single objects and short descriptive run slugs for multi-object batches. If you choose the worktree option from the default-branch prompt, that slug becomes the feature branch suffix.

## Commit granularity

Each item is committed as soon as it reaches a persisted non-error state in the loop. Items with status `error` are reverted inline before processing continues.

Per-item commits are pushed to remote immediately after committing.

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
- **Body:** created by the PR command; batch summaries remain in the command output and run logs

At the end of each batch command, you are offered the option to open a PR:

```text
All successful items committed and pushed.
Raise a PR for this run? (y/n)
```

PRs target the repo's default branch. The user reviews and merges — commands do not auto-merge.

## Interactive vs multi-table differences

| Aspect | Interactive workflow | Batch command |
|---|---|---|
| Branching | user's current branch | Current branch, or feature worktree if selected |
| Approval | Reviewed inline | Main-branch choice at start; per-item commits |
| PR creation | user manages manually | Command offers PR at end |
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
