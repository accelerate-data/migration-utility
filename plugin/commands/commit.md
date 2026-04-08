---
name: commit
description: Stage specific files, commit, and push to remote
user-invocable: true
argument-hint: "[files or message hint]"
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git commit:*), Bash(git push:*), Bash(git branch:*)
---

# Commit

Stage specific files, create a single focused commit, and push to remote. Never uses `git add .` or `git add -A`.

## Context

- Current status: !`git status`
- Staged and unstaged diff: !`git diff HEAD`
- Recent commits (for message style): !`git log --oneline -5`
- Current branch: !`git branch --show-current`

## Your task

1. If `$ARGUMENTS` names specific files, stage only those. Otherwise stage all modified tracked files individually by name — never `git add .`.
2. Write a concise commit message focused on *why*, not *what*. Follow the style of recent commits above.
3. Append the co-author trailer:

   ```text
   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   ```

4. Create the commit.
5. Push to origin with `-u` if the branch is not yet tracking a remote, otherwise `git push`.

## Constraints

- One concern per commit — if the diff spans unrelated concerns, stage only the files for one concern and tell the user what was left out.
- Never skip hooks (`--no-verify`).
- Never amend a published commit.
- Never force-push.
- Never reconfigure branch tracking (`git branch --set-upstream-to`, `git config branch.*`).
- **On failure: stop.** If `git push` or any other command fails, report the exact error to the user and stop. Never attempt workarounds or alternative push targets.
