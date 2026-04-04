# Git Workflow

The migration utility uses git worktrees, structured branch names, and commit-per-table granularity to keep multi-table batch work isolated and reviewable.

## Worktrees

Batch commands (`/scope`, `/profile`, `/generate-tests`, `/generate-model`) create worktrees to isolate their work from the main working tree. This lets the FDE run multiple commands in parallel without conflicts.

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

## Branch naming

| Mode | Branch pattern | Created by |
|---|---|---|
| Interactive (skill) | FDE's current branch | FDE manages manually |
| Multi-table (command) | `<command>-<table1>-<table2>-...` (truncated to 60 chars) | Command, before spawning sub-agents |

Single-table skill invocations (`/analyzing-table`, `/profiling-table`, etc.) do not create branches. The FDE works on whatever branch they are already on.

Multi-table commands create a branch and worktree automatically. Before creating a new one, the command scans for existing worktrees. If any are found, it lists them and asks the FDE whether to **continue on an existing worktree** (preserve prior work) or create a **new worktree**. This lets the FDE build up work across multiple command invocations — for example, scoping table A, then adding table B on the same branch. The branch name is built from the command name and the table names, truncated to 60 characters to stay within git limits.

## PR format

Pull requests follow a consistent format:

- **Title:** `VU-XXX: short description`
- **Body:** `Fixes VU-XXX` (one line per issue for multi-issue PRs)

PRs target the repo's default branch. The FDE reviews and merges -- commands do not auto-merge.

### PR body contents

Command-generated PRs include:

- Per-table status (success/skipped/error) from `summary.json`
- Diagnostics summary for any tables with warnings

## Commit granularity

One commit per table, on FDE approval. Commands aggregate results into `.migration-runs/summary.json` and present the summary to the FDE before committing anything.

### Commit message format

```text
<command>(<schema>.<table>): <one-line summary>
```

Examples:

```text
scope(silver.DimCustomer): resolve usp_load_dimcustomer as selected writer
profile(silver.DimProduct): classify as dim_scd1 with surrogate key
generate-model(silver.FactSales): generate stg_fact_sales with incremental materialization
```

## Interactive vs multi-table differences

| Aspect | Interactive (skill) | Multi-table (command) |
|---|---|---|
| Branching | FDE's current branch | Auto-created branch + worktree |
| Approval | Every step reviewed inline | Summary reviewed at end |
| PR creation | FDE manages manually | Command offers to open PR |
| Error handling | FDE handles directly | Skip-and-continue, surface in summary |

## What gets committed vs what stays local

| Committed (durable) | Never committed (ephemeral) |
|---|---|
| `catalog/tables/*.json` | `.migration-runs/` (`.gitignore`d) |
| `catalog/procedures/*.json` | |
| `test-specs/*.json` | |
| `dbt/models/**/*.sql` | |
| `dbt/models/**/*.yml` | |
| `ddl/*.sql` (from setup, not per-batch) | |

The `.migration-runs/` directory contains per-command execution metadata (timing, cost, per-item status). Per-item result files are overwritten on each run. It is never committed. It is consumed at commit/PR time for rich commit messages and PR bodies.

## Cleaning up worktrees

After a PR is merged, the worktree and its branches are no longer needed. Run `/cleanup-worktrees` to remove them. See [[Cleanup and Teardown]] for details.

## Related pages

- [[Cleanup and Teardown]] -- worktree and sandbox cleanup
- [[Status Dashboard]] -- checking pipeline progress
