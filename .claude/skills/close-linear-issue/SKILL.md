---
name: close-linear-issue
description: |
  Closes a completed Linear issue after testing. Merges the PR, moves the issue to Done, and cleans up.
  Triggers on "close <issue-id>", "complete <issue-id>", "merge <issue-id>", "ship <issue-id>", or "/close-issue".
---

# Close Linear Issue

Merge a PR, move related Linear issues to Done, and clean up branches/worktrees safely.

## Codex Execution Mode

See `../../rules/codex-execution-policy.md`.

## Tool Contract

Use these exact tools/commands:

- Linear: `mcp__linear__get_issue`, `mcp__linear__list_issues`, `mcp__linear__save_issue`, `mcp__linear__create_comment`
- GitHub CLI: `gh pr list`, `gh pr view`, `gh pr checks`, `gh pr merge`, `gh pr edit`
- Git: `git worktree remove`, `git branch -D`, `git push origin --delete`, `git pull`

Required fields:

- Linear close: `save_issue` with `id` + `state: "Done"`, plus closing comment containing PR URL and merge commit.
- PR data: fetch with `gh pr view --json number,url,body,state,mergeCommit,statusCheckRollup,baseRefName,headRefName`.

Fallback behavior:

- Retry failed API/CLI step once. If still failing, stop and report exact command and output.

## Required Checks Policy

Before merge:

1. Read branch protection/rulesets for the PR base branch.
2. List enforced required checks.
3. If required checks exist, wait on them via `gh pr checks --watch --required`.
4. If required checks list is empty, continue but post explicit risk comments on PR and Linear.

## Idempotency Rules

- If PR already merged, continue close flow without error.
- If issue already `Done`, skip state transition but still perform remaining cleanup/reporting.
- Avoid duplicate close comments by checking for existing comment containing same PR URL + merge SHA.
- If branch/worktree already removed, treat as success.

## Output Hygiene

- Use temp markdown files with `gh pr edit --body-file` for long PR updates.
- Never paste long command output into PR/Linear comments.

## Identify

1. Fetch issue details.
2. Find PR by `gitBranchName` (`gh pr list --head <branch>`).
3. Resolve worktree path `../worktrees/<branchName>`.
4. Child handling:
   - Fetch children via `mcp__linear__list_issues(parentId=issue.id)`.
   - Parse PR body for both `Fixes <id>` and `Closes <id>` keywords.
   - Any non-Done child not included in PR close keywords is a blocker.

## Verify Test Plan

Check PR body `## Test plan` section.

- If unchecked items exist, stop and report which items remain unchecked.
- If section is absent, stop and report the missing section.
- Continue only after the PR test plan reflects completed automated and manual validation.

## Merge

Direct merge execution is allowed.

1. First merge action (if PR is open): rebase branch onto `origin/main`.
2. Resolve conflicts when mechanical; escalate to user when judgment is needed.
3. Run `cd app && npx tsc --noEmit`.
4. Push with `--force-with-lease` if rebase changed history.
5. Apply required checks policy above.
6. Merge PR (prefer squash).
7. Capture merge commit SHA.

## Close

1. Move all linked issues to `Done`:
   - Primary issue plus every issue found in `Fixes`/`Closes` keywords.
2. Add closing Linear comment with PR URL and merge SHA.
3. Cleanup from main repo:
   - If worktree has uncommitted changes, capture `git status --porcelain` and ask user before force remove.
   - `git worktree remove --force <path>`
   - delete local branch
   - delete remote branch
   - pull latest `main`
4. Report closure status.

## Error Recovery

| Situation | Action |
| --- | --- |
| No PR found | Stop; instruct user to create PR via implement flow |
| Required checks failing | Stop and report failing checks |
| Merge conflict with semantic choice | Escalate to user |
| Worktree missing | Continue with PR + Linear close |
| Cleanup branch delete fails because already deleted | Continue |
