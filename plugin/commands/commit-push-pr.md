---
name: commit-push-pr
description: Commit, push, and open a PR. Derives title from step and tables context.
user-invocable: true
argument-hint: "[<step> <tables>] [files]"
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git commit:*), Bash(git push:*), Bash(git branch:*), Bash(gh pr create:*), Bash(gh pr list:*), Bash(gh pr edit:*)
---

# Commit, Push, and Open PR

Stage specific files, commit, push, and open a pull request.

## Context

- Current status: !`git status`
- Staged and unstaged diff: !`git diff HEAD`
- Current branch: !`git branch --show-current`
- Recent commits (for message style): !`git log --oneline -5`

## Your task

1. **Stage** specific files by name. Never use `git add .` or `git add -A`.
2. **Derive commit message** from the staged diff — read the diff, summarise *why* in a short conventional commit message. Append the co-author trailer:

   ```text
   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   ```

3. **Push** the current branch to origin with `-u` if not yet tracking.
4. **Derive PR title** from `$ARGUMENTS`:
   - If `$ARGUMENTS` contains a step and table list (e.g. `scope silver.DimCustomer,silver.DimProduct`), format as `<step>: <tables>` (e.g. `scope: silver.DimCustomer, silver.DimProduct`).
   - Otherwise infer the title from the diff subject line.
5. Check for an existing open PR: `gh pr list --head <branch> --state open --json number,url`
   - If one exists: update it with `gh pr edit <number> --title "<derived title>"`
   - If none: `gh pr create --title "<derived title>" --body ""`

## Constraints

- Never force-push. Never skip hooks.
- Never use `git add .` or `git add -A`.
- If already on `main`, stop and tell the user to create a branch first.
- Do not push to `main` directly — warn and abort if the current branch is `main`.
