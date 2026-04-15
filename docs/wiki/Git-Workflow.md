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

Batch commands create or reuse worktrees automatically through the internal `git-checkpoints` helper.

That helper bootstraps the worktree by:

- Symlinking `.env` from the main repo root
- Running `direnv allow`
- Running `uv sync --extra dev` in `lib/`
- Verifying `pyodbc` and `oracledb` import from the worktree venv
- Running `npm install --no-audit --no-fund` in `tests/evals/`

## Main-branch check

Every batch command checks the current branch at startup. If you are on the default branch, `git-checkpoints` asks whether to continue there or create a feature-branch worktree first.

If you continue on the default branch, the command runs in the current checkout. If you create a manual worktree first, the command uses it automatically on the next invocation.

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

Pull requests have titles derived from the command and tables processed:

- **Title:** `<command>: <table1>, <table2>` (e.g. `scope: silver.DimCustomer, silver.DimProduct`)
- **Body:** table-level summary from the run

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

## Committing and pushing

The scaffolded migration repo does not include `scripts/commit.sh` or `scripts/commit-push-pr.sh`.

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
| `test-specs/*.yml` | |
| `dbt/models/**/*.sql` | |
| `dbt/models/**/*.yml` | |
| `ddl/*.sql` (from setup, not per-batch) | |

The `.migration-runs/` directory contains per-command execution metadata (timing, cost, per-item status). Each file includes a Unix epoch suffix (e.g. `silver.dimcustomer.1743868200.json`) so runs accumulate without overwriting. It is never committed. The latest run's summary is consumed at PR time for rich PR bodies.

## Cleaning up worktrees

After a PR is merged, the worktree and its branches are no longer needed. Run `/cleanup-worktrees` to remove them.

## Related pages

- [[Sandbox Operations]] -- sandbox lifecycle commands
- [[Status Dashboard]] -- checking pipeline progress
