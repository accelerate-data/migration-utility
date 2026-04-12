# Worktree Wrapper

## Decision

Replace the two-step worktree flow with a single `scripts/worktree.sh` entrypoint.

## Why

One command is easier for humans and agents to invoke correctly, and it avoids half-configured worktrees caused by skipped or masked bootstrap failures.

## Behavior

`scripts/worktree.sh <branch>` will:

1. Derive the target path as `../worktrees/<branch>`.
2. Create missing parent directories for that path.
3. If `<branch>` does not exist, create it and add the worktree.
4. If `<branch>` exists and is not checked out in another worktree, add the worktree on that branch.
5. If `<branch>` is already checked out in another worktree, exit non-zero and emit structured JSON to `stderr` with the branch name, the requested worktree path, and the existing worktree path.
6. Bootstrap the worktree by:
   - symlinking `.env` from the main repo root when present
   - running `direnv allow` when available and `.envrc` exists
   - running `uv sync --extra dev` in `plugin/lib`
   - verifying `pyodbc` and `oracledb` import from the worktree venv
   - running `npm install --no-audit --no-fund` in `tests/evals`

If the requested branch is already attached at the derived worktree path, the script re-runs bootstrap for that worktree instead of failing.

## Failure Contract

Normal progress stays human-readable on stdout.

Failures emit one JSON object on stderr with stable fields:

- `code`
- `step`
- `message`
- `branch`
- `requested_worktree_path`
- `can_retry`
- `retry_command`
- `suggested_fix`

Some failures include `existing_worktree_path`.

Representative failure codes:

- `WORKTREE_BRANCH_ALREADY_CHECKED_OUT`
- `WORKTREE_DIRENV_ALLOW_FAILED`
- `WORKTREE_UV_SYNC_FAILED`
- `WORKTREE_DEPENDENCY_VERIFICATION_FAILED`
- `WORKTREE_NPM_INSTALL_FAILED`

## Safety

- The wrapper does not delete or move existing worktrees.
- The branch-already-checked-out case is a hard stop with structured guidance.
- Required setup failures stop immediately instead of continuing with a partial worktree.
