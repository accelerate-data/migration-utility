---
name: git-checkpoints
description: >
  Branch-safety skill. Checks whether the caller is on `main` and, if so,
  warns the user and offers to create a feature branch + worktree before
  migration work begins. No-op when already on a feature branch.
user-invocable: false
argument-hint: "<run-slug>"
---

# Git Checkpoints

Ensure the caller is not working directly on `main`. When invoked from a pipeline command, pass the run slug as `$ARGUMENTS` so the suggested worktree command uses the right branch name.

## Steps

### 1 — Check current branch

```bash
git branch --show-current
```

If the output is anything other than `main`, return immediately — no further action needed.

### 2 — Warn and offer worktree (main only)

Display the following block to the user:

```text
⚠️  You are on `main`. Migration work should run on a feature branch.

Suggested command to create a branch and worktree:

  mkdir -p ../worktrees/feature
  git worktree add ../worktrees/feature/<slug> -b feature/<slug>
  ./scripts/setup-worktree.sh ../worktrees/feature/<slug>

Where <slug> is: $ARGUMENTS

Create the branch and worktree now? (y to create / any other key to continue on main)
```

### 3 — Act on response

**If the user confirms (y):**

Run the three commands above, substituting `<slug>` with `$ARGUMENTS`:

```bash
mkdir -p ../worktrees/feature
git worktree add ../worktrees/feature/$ARGUMENTS -b feature/$ARGUMENTS
./scripts/setup-worktree.sh ../worktrees/feature/$ARGUMENTS
```

Return the worktree path: `../worktrees/feature/$ARGUMENTS`

All subsequent file writes and git operations for this run must target the worktree path.

**If the user skips (any other key):**

Return empty. The caller will continue operating on `main` — all git operations (commits, pushes) will target `main` directly. This is allowed but not recommended.

## Return Value

| Condition | Return |
|---|---|
| Created worktree | Absolute or relative path to the worktree |
| User skipped | Empty string |
| Already on a feature branch | Empty string (no-op) |
