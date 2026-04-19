---
name: commit-push-pr
description: Legacy helper for one-off commit, push, and PR creation. Coordinator-aware migration stage commands use shared/scripts/stage-pr.sh instead.
user-invocable: false
argument-hint: "[<step> <tables>] [files]"
---

# Commit, Push, and Open PR

Legacy helper for one-off commit, push, and PR creation. Coordinator-aware migration stage commands use shared/scripts/stage-pr.sh instead.

Do not call this command from /migrate-mart, /migrate-mart-plan, or coordinator-mode stage commands.

## Context

- Current status: !`git status`
- Staged and unstaged diff: !`git diff HEAD`
- Current branch: !`git branch --show-current`
- Recent commits (for message style): !`git log --oneline -5`

## Your task

### Step 0 — Detect default branch

Before anything else, detect the remote default branch:

```bash
default_branch=$(gh repo view --json defaultBranchRef -q .defaultBranchRef.name 2>/dev/null)
```

If `gh repo view` fails (e.g. no GitHub remote), fall back:

```bash
default_branch=$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's|refs/remotes/origin/||')
```

If both fail, **stop and report the error** — do not guess or assume `main`.

Store the result as `<default-branch>` for use in all subsequent steps.

### Step 1 — Stage and commit

1. **Stage** specific files by name. Never use `git add .` or `git add -A`.
2. **Derive commit message** from the staged diff — read the diff, summarise *why* in a short conventional commit message. Append the co-author trailer:

   ```text
   Co-Authored-By: Claude <noreply@anthropic.com>
   ```

### Step 2 — Push

**Push** the current branch to origin with `-u` if not yet tracking.

### Step 3 — Open or update PR

1. **Derive PR title** from `$ARGUMENTS`:
   - If `$ARGUMENTS` contains a step and table list (e.g. `scope silver.DimCustomer,silver.DimProduct`), format as `<step>: <tables>` (e.g. `scope: silver.DimCustomer, silver.DimProduct`).
   - Otherwise infer the title from the diff subject line.
2. Check for an existing open PR: `gh pr list --head <branch> --state open --json number,url`
   - If one exists: update it with `gh pr edit <number> --title "<derived title>"`
   - If none: `gh pr create --base <default-branch> --title "<derived title>" --body ""`

## Constraints

- Never force-push. Never skip hooks.
- Never use `git add .` or `git add -A`.
- If already on `<default-branch>`, stop and tell the user to create a branch first.
- Do not push to `<default-branch>` directly — warn and abort if the current branch is `<default-branch>`.
- Never run `git push origin <default-branch>` from a feature branch.
- Never reconfigure branch tracking (`git branch --set-upstream-to`, `git config branch.*`).
- **On failure: stop.** If `gh pr create`, `gh pr edit`, `git push`, or any other command fails, report the exact error to the user and stop. Never attempt workarounds, recovery steps, or alternative push targets.
