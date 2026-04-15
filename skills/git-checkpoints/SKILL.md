---
name: git-checkpoints
description: >
  Branch-safety skill. Checks whether the caller is on the remote default branch
  and, if so, asks the user to choose the default branch or create a feature
  branch + worktree before migration work begins. Returns the working tree path
  when on a feature branch, the default branch name when the user explicitly
  chose it, or the new worktree path when one is created.
user-invocable: false
argument-hint: "<run-slug>"
---

# Git Checkpoints

Ensure the caller is not working directly on the remote default branch without an explicit choice. When invoked from a pipeline command, pass the run slug as `$ARGUMENTS` so the branch name reflects the intent of the run.

## Steps

### 0 — Detect default branch

Before checking anything, detect the remote default branch:

```bash
default_branch=$(gh repo view --json defaultBranchRef -q .defaultBranchRef.name 2>/dev/null)
```

If `gh repo view` fails, fall back:

```bash
default_branch=$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's|refs/remotes/origin/||')
```

If both fail, **stop and report the error** — do not guess or assume `main`.

Store the result as `<default-branch>` for all subsequent steps.

### 1 — Check current branch

```bash
git branch --show-current
```

If the output is anything other than `<default-branch>`:

```bash
git rev-parse --show-toplevel
```

Return that path immediately — no further action needed.

### 2 — Ask the user (default branch only)

Use `AskUserQuestion` to present a single choice:

> **Question:** "You are on `<default-branch>`. Work on `<default-branch>`, or create a feature branch with a worktree?"
>
> **Option 1 — "Continue on `<default-branch>`":** proceed without a branch or worktree; all git operations target the current directory.
>
> **Option 2 — "Create branch: feature/`$ARGUMENTS`":** create a new branch and worktree now.
>
> (The user may type a custom branch name via the "Other" field to override the suggested slug.)

**If `AskUserQuestion` is unavailable** (e.g. running inside a sub-agent with no interactive tool access): fail loudly — output an error and stop. Do not silently skip or pick a default.

### 3a — User chose the default branch

Return the detected `<default-branch>` name (e.g. `"main"`, `"master"`). No branch, no worktree.

### 3b — User chose a feature branch

Determine the slug: use `$ARGUMENTS` unless the user typed a custom value via "Other". The full branch name is `feature/<slug>` unless the user typed a complete `feature/...` name.

Resolve the repo root absolutely:

```bash
repo_root=$(git rev-parse --show-toplevel)
```

Create the worktree:

```bash
cd "$repo_root"
"$repo_root/skills/git-checkpoints/scripts/worktree.sh" "feature/<slug>"
```

Determine the worktree path as `$repo_root/../worktrees/feature/<slug>` and return that absolute
path. All subsequent file writes and git operations for this run must target that path.

## Return Value

| Condition | Return |
|---|---|
| Already on a feature branch | `git rev-parse --show-toplevel` output (current tree root) |
| User chose the default branch | The detected `<default-branch>` name (e.g. `"main"`, `"master"`) |
| User created a new worktree | Absolute path to the new worktree |

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `gh repo view` | non-zero | Fall back to `git symbolic-ref` |
| `git symbolic-ref` | non-zero | Stop — cannot detect default branch |
| `git worktree add` | non-zero | Report error and stop |
