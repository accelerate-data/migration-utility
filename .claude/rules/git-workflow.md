# Git Workflow

## Worktrees

Worktrees live at `../worktrees/<branchName>` relative to the repo root, preserving the full branch name including the `feature/` prefix.

Example:

- Branch: `feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`
- Worktree path: `../worktrees/feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`

Create or attach a worktree with:

```bash
./scripts/worktree.sh <branch-name>
```

`worktree.sh` creates or attaches the worktree at `../worktrees/<branch-name>` and then bootstraps it:

1. Symlinks `.env` from the main repo root
2. Runs `direnv allow` when available
3. Runs `uv sync --extra dev` in `lib/`
4. Verifies `pyodbc` and `oracledb` import from the worktree venv
5. Runs `npm ci --no-audit --no-fund` in `tests/evals/` when `package-lock.json` exists, otherwise falls back to `npm install --no-audit --no-fund`

It fails fast if a required setup step breaks so the worktree is not left half-configured. When the
branch is already checked out in a different worktree, it exits with structured JSON on stderr that
identifies the existing checkout path.

## PR Format

- Title: `VU-XXX: short description`
- Body: `Fixes VU-XXX` (one line per issue for multi-issue PRs)
