---
name: git-checkpoints
description: >
  Branch-safety skill. Checks whether the caller is on `main` and, if so,
  asks the user to choose main or create a feature branch + worktree before
  migration work begins. Returns the working tree path when on a feature branch,
  "main" when the user explicitly chose main, or the new worktree path when
  one is created.
user-invocable: false
argument-hint: "<run-slug>"
---

# Git Checkpoints

Ensure the caller is not working directly on `main` without an explicit choice. When invoked from a pipeline command, pass the run slug as `$ARGUMENTS` so the branch name reflects the intent of the run.

## Steps

### 1 — Check current branch

```bash
git branch --show-current
```

If the output is anything other than `main`:

```bash
git rev-parse --show-toplevel
```

Return that path immediately — no further action needed.

### 2 — Ask the user (main only)

Use `AskUserQuestion` to present a single choice:

> **Question:** "You are on `main`. Work on main, or create a feature branch with a worktree?"
>
> **Option 1 — "Continue on main":** proceed without a branch or worktree; all git operations target the current directory.
>
> **Option 2 — "Create branch: feature/`$ARGUMENTS`":** create a new branch and worktree now.
>
> (The user may type a custom branch name via the "Other" field to override the suggested slug.)

**If `AskUserQuestion` is unavailable** (e.g. running inside a sub-agent with no interactive tool access): fail loudly — output an error and stop. Do not silently skip or pick a default.

### 3a — User chose main

Return the literal string `"main"`. No branch, no worktree.

### 3b — User chose a feature branch

Determine the slug: use `$ARGUMENTS` unless the user typed a custom value via "Other". The full branch name is `feature/<slug>` unless the user typed a complete `feature/...` name.

Resolve the repo root absolutely:

```bash
repo_root=$(git rev-parse --show-toplevel)
```

Determine the worktree path: `$repo_root/../worktrees/feature/<slug>`.

Create the worktree:

```bash
mkdir -p "$repo_root/../worktrees/feature"
git worktree add "$repo_root/../worktrees/feature/<slug>" -b "feature/<slug>"
```

Inline setup (symlink `.env` and run `direnv allow`):

```bash
env_src="$repo_root/.env"
env_dst="$repo_root/../worktrees/feature/<slug>/.env"
if [ -f "$env_src" ]; then
  ln -sf "$env_src" "$env_dst"
  echo "ENV: symlinked $env_dst -> $env_src"
else
  echo "ENV: skipped (no .env in $repo_root)"
fi

worktree_path="$repo_root/../worktrees/feature/<slug>"
if command -v direnv >/dev/null 2>&1 && [ -f "$worktree_path/.envrc" ]; then
  direnv allow "$worktree_path"
  echo "direnv: allowed $worktree_path"
else
  echo "direnv: skipped"
fi
```

Return the absolute worktree path. All subsequent file writes and git operations for this run must target that path.

## Return Value

| Condition | Return |
|---|---|
| Already on a feature branch | `git rev-parse --show-toplevel` output (current tree root) |
| User chose main | `"main"` (literal string) |
| User created a new worktree | Absolute path to the new worktree |
