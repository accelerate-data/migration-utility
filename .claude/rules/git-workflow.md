# Git Workflow

## Worktrees

Worktrees live at `../worktrees/<branchName>` relative to the repo root, preserving the full branch name including the `feature/` prefix.

Example:

- Branch: `feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`
- Worktree path: `../worktrees/feature/vu-354-scaffold-tauri-app-with-full-frontend-stack`

Create or attach a maintainer development worktree with the repo-root helper:

```bash
./scripts/worktree.sh <branch-name>
```

The helper owns maintainer worktree setup and may change its bootstrap behavior over time.

Do not use `scripts/stage-worktree.sh` for maintainer development worktrees. That script is bundled
as part of the plugin runtime and is called by customer-project slash commands with explicit branch,
worktree, and base-branch inputs.

## PR Format

- Title: `VU-XXX: short description`
- Body: `Fixes VU-XXX` (one line per issue for multi-issue PRs)
