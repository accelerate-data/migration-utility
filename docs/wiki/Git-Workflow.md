# Git Workflow

The migration utility uses git worktrees, structured branch names, and automatic per-table commits to keep multi-table batch work isolated and reviewable.

## Worktrees

Batch commands (`/scope`, `/profile`, `/generate-tests`, `/refactor`, `/generate-model`) create worktrees to isolate their work from the main working tree. This lets the FDE run multiple commands in parallel without conflicts.

Worktrees are created at `../worktrees/<branchName>` relative to the repo root. For feature branches, the full prefix is preserved:

```text
../worktrees/feature/vu-354-scaffold-tauri-app
```

For batch command worktrees, the branch name is derived from the command and tables:

```text
../worktrees/feature/scope-silver-dimcustomer-silver-dimproduct
```

The `setup-worktree.sh` script runs after worktree creation and handles two things:

- Symlinks `.env` from the main repo root
- Runs `direnv allow`

## Main-branch check

Every batch command checks the current branch at startup. If you are on `main`, the command warns and offers to create a worktree:

```text
⚠️  You are on main. It is recommended to work on a feature branch.

To create a worktree now:
  mkdir -p ../worktrees/feature
  git worktree add ../worktrees/feature/<slug> -b feature/<slug>
  ./scripts/setup-worktree.sh ../worktrees/feature/<slug>

Proceed on main anyway? (y/n)
```

If you confirm, the command continues on `main`. If you create the worktree first, the command uses it automatically on the next invocation.

## Branch naming

| Mode | Branch pattern | Created by |
|---|---|---|
| Interactive (skill) | FDE's current branch | FDE manages manually |
| Multi-table (command) | `<command>-<table1>-<table2>-...` (truncated to 60 chars) | Command, before spawning sub-agents |

Single-table skill invocations (`/analyzing-table`, `/profiling-table`, etc.) do not create branches. The FDE works on whatever branch they are already on.

Multi-table commands create a branch and worktree automatically. Before creating a new one, the command scans for existing worktrees. If any are found, it lists them and asks the FDE whether to **continue on an existing worktree** (preserve prior work) or create a **new worktree**. This lets the FDE build up work across multiple command invocations — for example, scoping table A, then adding table B on the same branch. The branch name is built from the command name and the table names, truncated to 60 characters to stay within git limits.

## Commit granularity

Each table is committed automatically as soon as it reaches its final state in the loop — no batch approval step. Only items with status `ok` or `partial` are committed. Items with status `error` are reverted inline: `git checkout -- <files>` runs immediately after the failure, before processing the next table.

Per-table commits are pushed to remote immediately after committing.

### Commit message format

```text
<command>(<schema>.<table>): <one-line summary>
```

Examples:

```text
scope(silver.DimCustomer): resolve usp_load_dimcustomer as selected writer
profile(silver.DimProduct): classify as dim_scd1 with surrogate key
refactor(silver.DimCustomer): 4 CTEs, audit passed
generate-model(silver.FactSales): stg_factsales + mart, incremental materialization
```

## PR format

Pull requests have titles derived from the command and tables processed — no Linear issue ID required when working in a customer repo:

- **Title:** `<command>: <table1>, <table2>` (e.g. `scope: silver.DimCustomer, silver.DimProduct`)
- **Body:** table-level summary from the run

If you are working on a development issue with a Linear ID, pass it directly: `/commit-push-pr VU-XXX`.

At the end of each batch command, you are offered the option to open a PR:

```text
All successful items committed and pushed.
Raise a PR for this run? (y/n)
```

PRs target the repo's default branch. The FDE reviews and merges — commands do not auto-merge.

## Interactive vs multi-table differences

| Aspect | Interactive (skill) | Multi-table (command) |
|---|---|---|
| Branching | FDE's current branch | Auto-created branch + worktree |
| Approval | Every step reviewed inline | Main-branch check at start; auto-commit per table |
| PR creation | FDE manages manually | Command offers PR at end |
| Error handling | FDE handles directly | Inline revert per error, surface in summary |

## What gets committed vs what stays local

| Committed (durable) | Never committed (ephemeral) |
|---|---|
| `catalog/tables/*.json` | `.migration-runs/` (`.gitignore`d) |
| `catalog/procedures/*.json` | |
| `test-specs/*.json` | |
| `dbt/models/**/*.sql` | |
| `dbt/models/**/*.yml` | |
| `ddl/*.sql` (from setup, not per-batch) | |

The `.migration-runs/` directory contains per-command execution metadata (timing, cost, per-item status). Each file includes a Unix epoch suffix (e.g. `silver.dimcustomer.1743868200.json`) so runs accumulate without overwriting. It is never committed. The latest run's summary is consumed at PR time for rich PR bodies.

## Cleaning up worktrees

After a PR is merged, the worktree and its branches are no longer needed. Run `/cleanup-worktrees` to remove them. See [[Cleanup and Teardown]] for details.

## Related pages

- [[Cleanup and Teardown]] -- worktree and sandbox cleanup
- [[Status Dashboard]] -- checking pipeline progress
