---
name: commit-push-pr
description: Commit, push, and open a PR following VU-XXX conventions
user-invocable: true
argument-hint: "<VU-XXX> [files]"
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git commit:*), Bash(git push:*), Bash(git branch:*), Bash(gh pr create:*)
---

# Commit, Push, and Open PR

Stage specific files, commit, push, and open a pull request using project conventions.

## Context

- Current status: !`git status`
- Staged and unstaged diff: !`git diff HEAD`
- Current branch: !`git branch --show-current`
- Recent commits (for message style): !`git log --oneline -5`

## Your task

`$ARGUMENTS` must contain a Linear issue identifier (e.g. `VU-123`). Extract it — call it `$ISSUE`.

1. **Stage** specific files by name. Never use `git add .` or `git add -A`.
2. **Commit** with a message following recent commit style. Append the co-author trailer:
   ```
   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   ```
3. **Push** the current branch to origin with `-u` if not yet tracking.
4. **Open a PR** using:
   ```
   gh pr create \
     --title "$ISSUE: <short description>" \
     --body "Fixes $ISSUE"
   ```

Execute all steps in a single message with parallel tool calls where possible.

## Constraints

- PR title format is strictly `VU-XXX: short description` (under 70 characters).
- Never force-push. Never skip hooks.
- If already on `main`, stop and tell the user to create a branch first.
- Do not push to `main` directly — warn and abort if the current branch is `main`.
