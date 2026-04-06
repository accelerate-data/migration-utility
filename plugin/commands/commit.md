---
name: commit
description: Stage specific files and create a granular git commit following project conventions
user-invocable: true
argument-hint: "[files or message hint]"
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git commit:*)
---

# Commit

Stage specific files and create a single focused commit. Never uses `git add .` or `git add -A`.

## Context

- Current status: !`git status`
- Staged and unstaged diff: !`git diff HEAD`
- Recent commits (for message style): !`git log --oneline -5`
- Current branch: !`git branch --show-current`

## Your task

1. If `$ARGUMENTS` names specific files, stage only those. Otherwise stage all modified tracked files individually by name — never `git add .`.
2. Write a concise commit message focused on *why*, not *what*. Follow the style of recent commits above.
3. Append the co-author trailer:
   ```
   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   ```
4. Create the commit. Do not push. Do not open a PR.

Execute steps 1 and 3 in a single message with parallel tool calls where possible.

## Constraints

- One concern per commit — if the diff spans unrelated concerns, stage only the files for one concern and tell the user what was left out.
- Never skip hooks (`--no-verify`).
- Never amend a published commit.
