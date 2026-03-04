# Git Workflow

## Worktrees

Worktrees live at `../worktrees/<branchName>` relative to the repo root, preserving the full branch name including the `feature/` prefix.

Example:

- Branch: `feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`
- Worktree path: `../worktrees/feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`

Pre-create the parent directory before adding the worktree:

```bash
mkdir -p ../worktrees/feature
git worktree add ../worktrees/feature/<branch-name> <branch-name>
./scripts/link-worktree-db.sh ../worktrees/feature/<branch-name>
```

The symlink command creates `../worktrees/<branchName>/.local/migration-utility.db` pointing to the
developer SQLite DB (`~/Library/Application Support/com.vibedata.migration-utility/migration-utility.db`).
If your DB path is different, set `MIGRATION_UTILITY_DB_PATH` before running the command.

## PR Format

- Title: `VU-XXX: short description`
- Body: `Fixes VU-XXX` (one line per issue for multi-issue PRs)
