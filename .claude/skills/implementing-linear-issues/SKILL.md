---
name: implementing-linear-issues
description: Use when implementing a Linear issue in this repository after issue creation is complete and coding should stop before PR creation
---

# Implementing Linear Issues

## Overview

Implement the approved issue, but stop before the PR phase. This skill owns mandatory branch/worktree setup, codebase-first clarification, plan approval, isolated implementation work, local verification, and Linear implementation updates. `raising-linear-prs` owns the rebase, quality gates, commit, push, acceptance-criteria checkoff, PR creation, and `In Review` transition.

## When to Use

- User asks to implement, fix, build, or work on a Linear issue.
- The issue already exists and needs code changes.
- Do not use for ticket drafting, PR raising, or close/merge work.

## Quick Reference

| Step | Requirement |
|---|---|
| 1 | Create or reuse the issue branch and worktree before any implementation work |
| 2 | Stop immediately if branch or worktree setup fails |
| 3 | Read the Linear issue and search the codebase before asking anything |
| 4 | Resolve every answerable clarification yourself |
| 5 | Ask one question at a time only for true forks |
| 6 | Enter plan mode and show the full plan before coding |
| 7 | Implement, test, and hand off before commit/PR |

## Implementation

**Tool contract:** use `mcp__codex_apps__linear_mcp_server_get_issue`, `list_issues`, `list_comments`, `save_issue`, `save_comment`, `git branch`, `git worktree`, `git status`, and repo test commands from `repo-map.json`. Retry once on tool failure, then stop and report the exact failing step.

**Status setup:**

- Stop on `Done`, `Cancelled`, or `Duplicate`.
- Move `Todo` to `In Progress` and assign to `me`.
- Move `In Review` back to `In Progress` before continuing.

**Branch and worktree setup is mandatory and non-negotiable:**

- Always create or reuse the issue branch and worktree before any implementation work starts, regardless of issue size.
- Do not make code changes in the main checkout.
- Do not inspect or edit target files from the main checkout once branch/worktree setup begins.
- If branch creation fails, stop immediately.
- If worktree creation fails, stop immediately.
- Never continue implementation after a branch or worktree setup failure.
- There is no small-issue exception and no negotiation on this step.

**Clarification protocol:**

1. Create or reuse the issue branch and worktree.
2. If branch or worktree setup fails, stop and report the exact failing command.
3. Read the issue from Linear.
4. Search the codebase before asking the user anything.
5. For each open question:
   - If the code answers it confidently, state the decision and continue.
   - If exactly one viable path exists, state it and continue.
   - If two or more viable paths exist, present them, recommend one, ask one question, and wait.
6. Never batch questions.
7. Never enter plan mode while any gap remains unresolved.

**Plan mode is required.** The implementation plan must include:

- chosen approach and rejected alternatives when relevant
- files or modules expected to change inside the isolated worktree
- test coverage by layer: unit, integration, eval
- skill or plugin eval coverage affected by the change
- manual test scope, or the explicit statement `No manual tests required.`

Post the approved plan to Linear before coding. If the user rejects it, revise and re-enter plan mode.

**Implementation rules:**

- Branch and worktree setup happens before any file edits.
- Implementation happens inside the worktree only.
- Stay within issue scope. Pause if work escapes the ticket boundary.
- Read existing tests before changing them.
- Add logging for new behavior per repo policy.
- Update the issue description or implementation comment as a living snapshot.
- Output `✅ <completed step>` after each major implementation step.

**Stop conditions:**

- permanent file deletion
- new external dependency
- architecture-impacting fork
- unresolved error after two attempts
- required changes outside issue scope

**Handoff boundary:**

- Do not create commits.
- Do not push.
- Do not create or update a PR.
- Do not move the issue to `In Review`.
- Hand off to `raising-linear-prs` after local verification passes.
- Report the branch name, worktree path, verification run, and any remaining risks.

## Common Mistakes

- Starting changes in the main checkout instead of a dedicated branch and worktree.
- Continuing after branch or worktree setup fails.
- Asking the user questions the codebase could answer.
- Entering plan mode before clarification is complete.
- Treating this skill as the PR phase.
- Checking off acceptance criteria before the PR phase quality gates run.

## Error Recovery

| Situation | Action |
|---|---|
| Worktree exists on wrong branch | Remove and recreate before continuing |
| Branch or worktree creation fails | Stop immediately and report the exact failure |
| Linear API fails | Retry once, then stop with details |
| Tests fail after 3 attempts | Escalate with failure details |
| Plan rejected twice | Ask user for explicit direction |

---

## Exit State

When this skill finishes, the branch and worktree exist, the implementation is complete, local verification has run, and the issue is still in `In Progress`. The next step is `raising-linear-prs`.
