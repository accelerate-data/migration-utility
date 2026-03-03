---
name: implement-linear-issue
description: |
  Implements a Linear issue end-to-end, from planning through PR creation.
  Triggers on "implement <issue-id>", "work on <issue-id>", "working on <issue-id>", "build <issue-id>", "fix <issue-id>", or "/implement-issue".
  Also triggers when the user simply mentions a Linear issue identifier (e.g. "ABC-123").
---

# Implement Linear Issue

Implement a Linear issue end-to-end and produce a review-ready PR.

## Codex Execution Mode

See `../../rules/codex-execution-policy.md`.

## Tool Contract

Use these exact tools/commands:

- Linear: `mcp__linear__get_issue`, `mcp__linear__list_issues`, `mcp__linear__save_issue`, `mcp__linear__create_comment`
- GitHub CLI: `gh pr create`, `gh pr edit`, `gh pr view`, `gh pr checks`
- Git: `git worktree`, `git status`, `git add`, `git commit`, `git push`

Required fields:

- Status transition via `save_issue`: `id`, `state`; include `assignee: "me"` when moving to active work.
- PR validation: `gh pr view --json url,number,body,state,headRefName,baseRefName,statusCheckRollup`

Fallback behavior:

- If a required Linear/GitHub operation fails after one retry, stop and report the exact failed step and command.

## Required Checks Policy

Before moving issue to `In Review`, inspect required checks for the base branch.

- If required checks exist: ensure PR state/checks satisfy them.
- If required checks list is empty: continue, but post explicit risk comments on PR and Linear stating no required checks are enforced.

## Idempotency Rules

- Re-runs must be safe:
  - Do not duplicate implementation notes comments if an equivalent note already exists.
  - Do not reopen `Done/Cancelled/Duplicate` issues.
  - If PR already exists for branch, update it instead of creating a new one.
  - If worktree already exists on the correct branch, reuse it.

## Output Hygiene

- Always write PR bodies/long comments to temp markdown files and use `--body-file`.
- Never inline long command outputs into PR body, Linear description, or comments.

## Autonomy

Do not ask permission for non-destructive work. Only confirm with user:

- Implementation plan for non-trivial scope
- Scope changes discovered during implementation
- Final status before moving to review

## Setup

1. Fetch issue via `mcp__linear__get_issue`.
2. Check child issues via `mcp__linear__list_issues(parentId=issue.id)`.
3. Status guard:
   - `Done/Cancelled/Duplicate`: stop.
   - `Todo`: assign to me + move to `In Progress`.
   - `In Progress`: continue (assign to me if missing).
   - `In Review`: move back to `In Progress`.
4. Create or reuse worktree at `../worktrees/<branchName>`.

## Approach Selection

- XS/S + isolated changes: implement directly.
- M+ or multi-component: create a short plan, then execute with parallelism where useful.
- User can always override.

## Branch Sync (required)

Before running Quality Gates, rebase the working branch onto `origin/main`.

1. Fetch latest `origin/main`.
2. Rebase current branch onto `origin/main`.
3. Resolve conflicts when mechanical; escalate to user when semantic judgment is required.
4. Push with `--force-with-lease` if history changed.

## Quality Gates

```text
Quality Gates:
- [ ] Tests written
- [ ] Tests passing
- [ ] Logging compliant
- [ ] Brand compliant
- [ ] Code simplified (optional for large diffs)
- [ ] Code reviewed
- [ ] Docs updated
- [ ] Final validation
```

### Tests written

Add targeted tests for changed behavior only. Update/remove obsolete tests.

### Tests passing

Run:

1. `cd app && npx tsc --noEmit`
2. Test commands based on changed areas per repo guidelines.

### Logging compliant

Confirm changed code follows repo logging rules.

### Brand compliant

Run the repo's off-brand color grep check for changed frontend files.

### Code simplified (optional)

- Optional by default.
- Required only when diff is large (roughly >5 files or >300 LOC) or readability is clearly degraded.

### Code reviewed

Run a focused code review pass (directly or via sub-agent).

### Docs updated

Update docs only where behavior/commands/conventions changed.

### Final validation

Run final relevant tests after fixes/review.

## Completion

1. Verify checklist coverage against issue description:
   - Evaluate every issue checkbox under Scope / Requirements / AC / Test Notes.
   - Check only items that are demonstrably implemented in code/tests.
   - Leave unverifiable or partial items unchecked.
2. Sync Linear checklist state:
   - Update issue description checkboxes (`[ ]`/`[X]`) via `mcp__linear__save_issue`.
   - Add one concise Linear comment with evidence for each newly checked item (file/test refs).
3. Create/update PR:
   - Use `gh pr create/edit --body-file <tmp.md>`.
   - PR body must include `Fixes <issue-id>` lines for primary issue and any included child issues.
4. Harden PR link:
   - Run `gh pr view --json ...` and verify PR exists, is open, and `Fixes <issue-id>` entries are present.
5. Generate concise implementation notes from:
   - `git log --oneline main..HEAD`
   - `git diff --stat main...HEAD`
6. Post implementation notes to Linear.
7. Branch protection awareness:
   - Read required checks state; enforce policy above.
8. Move issue(s) to `In Review`.
9. Report PR URL, worktree path, recommended test mode, and manual test steps.
10. Do not remove worktree in implement flow.

## Error Recovery

| Situation | Action |
| --- | --- |
| Worktree exists on wrong branch | Remove and recreate |
| Linear API fails | Retry once, then stop with details |
| Tests fail after 3 attempts | Escalate with failure details |
| ACs remain unmet | Keep `In Progress` and report gaps |
