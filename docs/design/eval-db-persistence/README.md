# Eval DB Persistence Across Worktrees

## Problem

Promptfoo stores eval results in a SQLite database at the path set by `PROMPTFOO_CONFIG_DIR`. The eval wrapper script (`tests/evals/scripts/promptfoo.sh`) sets this to `$SCRIPT_DIR/.promptfoo`, which resolves to the worktree-local `tests/evals/.promptfoo/`. Each worktree gets an isolated, empty database. When the worktree is cleaned up, results are lost.

This makes it impossible to profile turn usage (`numTurns`) across runs, which is needed to right-size `max_turns` budgets per eval test (VU-1066).

## Decision

Do not couple eval DB persistence to worktree bootstrap. Worktree bootstrap stays focused on repo setup, and `tests/evals/.promptfoo/` remains worktree-local unless the eval harness itself is changed.

## Why symlink instead of alternatives

- **Changing `PROMPTFOO_CONFIG_DIR` to `~/.promptfoo/`**: Mixes results from all projects on the machine. Filtering by project requires parsing eval descriptions, which is fragile.
- **Resolving `PROMPTFOO_CONFIG_DIR` via `git-common-dir`**: Requires modifying `promptfoo.sh` and only fixes the DB path.
- **Committing the DB to git**: SQLite binaries don't diff, grow unboundedly, and merge conflicts are unresolvable.

## Profiling approach

The claude-agent-sdk provider already writes `numTurns` into the `metadata` field of each eval result (via the `metadata.numTurns` key on the promptfoo `ProviderResponse`). Once results accumulate in the shared DB, turn usage can be queried directly from the `evals` table's `results` JSON column. No custom instrumentation is needed.

## Concurrency note

SQLite can lock under concurrent writes. Parallel eval runs from different worktrees writing to the same DB may hit `SQLITE_BUSY`. Promptfoo may silently drop results rather than crash on a locked DB, so concurrent runs risk silent data loss rather than a visible error. This is unlikely in practice (evals are rarely run in parallel across worktrees) and acceptable for this use case.
