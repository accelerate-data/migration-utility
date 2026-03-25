---
name: implement-linear-issue
description: |
  Implements a Linear issue end-to-end, from planning through PR creation.
  Triggers on "implement <issue-id>", "work on <issue-id>", "working on <issue-id>", "build <issue-id>", "fix <issue-id>", or "/implement-issue".
  Also triggers when the user simply mentions a Linear issue identifier (e.g. "ABC-123").
---

# Implement Linear Issue

Implement a Linear issue end-to-end and produce a review-ready PR.

See `../../rules/codex-execution-policy.md` for execution mode.

## Flow Overview

```text
User mentions issue
  │
  1) Setup ──► fetch issue, status guard, worktree
  │
  2) Planning ──► draft plan, post to Linear, get user approval
  │
  3) Implementation
  │    ├─ XS/S isolated ──► single-agent fast path
  │    └─ M+ or multi-component ──► parallel work streams
  │
  4) Branch Sync ──► rebase onto origin/main
  │
  5) Quality Gates ──► tests, logging, brand, review, docs
  │
  6) Completion ──► update Linear, create/update PR, move to In Review
```

Every run posts a plan to Linear before coding, and posts implementation notes to Linear after coding.

---

## 1) Tool Contract

Use these exact tools/commands:

| Tool / Command | Purpose |
|---|---|
| `mcp__linear__get_issue` | fetch issue details |
| `mcp__linear__list_issues` | dedupe, child discovery |
| `mcp__linear__save_issue` | status transitions, AC updates, description updates |
| `mcp__linear__save_comment` | plan comment, implementation notes |
| `mcp__linear__list_comments` | check for existing plan |
| `gh pr create`, `gh pr edit`, `gh pr view`, `gh pr checks` | PR lifecycle |
| `git worktree`, `git status`, `git add`, `git commit`, `git push` | version control |

Required fields for `save_issue`: `id`, `state`; include `assignee: "me"` when moving to active work.

**Fallback:** if a required tool fails after one retry, stop and report the exact failed step.

---

## 2) Core Rules

### Idempotency

- Do not duplicate plan or implementation comments if equivalent already exists.
- Do not reopen `Done/Cancelled/Duplicate` issues.
- If PR already exists for branch, update it instead of creating a new one.
- If worktree already exists on the correct branch, reuse it.

### Output hygiene

- Write PR bodies and long comments to temp markdown files; use `--body-file`.
- Never inline long command output into PR body, Linear description, or comments.

### Autonomy

Do not ask permission for non-destructive work. Only confirm with user:

- Plan approval (always required — see Planning section)
- Scope changes discovered during implementation
- Final status before moving to review

### Linear update rules

- **Rewrite, don't append.** The Implementation Updates section is a living snapshot, not an audit log.
- **Never remove acceptance criteria.** Check them off or add new ones.
- **Preserve the original issue description.** Append the Implementation Updates section below it.
- **Coding agents** check off their ACs on Linear after tests pass.
- **Coordinator** owns the Implementation Updates section (prevents race conditions).

---

## 3) Setup

1. Fetch issue via `mcp__linear__get_issue`.
2. Check child issues via `mcp__linear__list_issues(parentId=issue.id)`.
3. Status guard:
   - `Done/Cancelled/Duplicate`: stop.
   - `Todo`: assign to me + move to `In Progress`.
   - `In Progress`: continue (assign to me if missing).
   - `In Review`: move back to `In Progress`.
4. Create or reuse worktree at `../worktrees/<branchName>`. If creating new, run `./scripts/link-worktree-db.sh <worktree-path>` immediately after `git worktree add`.
5. Fetch comments via `mcp__linear__list_comments`. If a comment containing `## Implementation Plan` exists, load it as the active plan and skip to getting user approval (or resume execution if already approved).

---

## 4) Planning (required for all issues)

Planning is required for every issue, regardless of size. The depth scales with complexity, but the plan-post-approve cycle is always followed.

### Step 1 — Draft the plan

**For XS/S (isolated changes):**

```md
## Implementation Plan

### Approach
[1-2 sentences: what will change and why]

### Files
- `path/to/file` — what to create or change

### Test Strategy
- Update/remove/add: [which test files]
- Run: [which test commands]

### Notes
- [Key decisions or constraints, if any]
```

**For M+ (multi-component):**

```md
## Implementation Plan

### Approach
[Summary of the approach]

### Work Streams
1. **[Stream name]** — [what it does], owns AC: [list]
   - Dependencies: [or "none"]
2. **[Stream name]** — ...

### AC Mapping
- [ ] AC 1 → Stream 1
- [ ] AC 2 → Stream 2
[Flag any ACs not covered by a stream]

### Test Strategy
Per stream:
- **Update**: existing test files that need changes
- **Remove**: tests that become redundant
- **Add**: new test files for new behavior
- **Run**: full set of tests after implementation

### Risk Notes
- [Shared files, potential conflicts between streams]

### Logging Plan
- [New Rust commands needing `info!`/`error!`, frontend actions needing `console.*`]
```

### Step 2 — Post to Linear

Post the plan as a comment via `mcp__linear__save_comment`. Enter plan mode and present the plan to the user for approval.

### Step 3 — User approval

Wait for user approval before writing any code. If the user rejects the plan:

1. Revise based on feedback.
2. Present 2-3 alternative approaches with trade-offs if the direction changed.
3. If the chosen approach changes requirements or ACs, update the Linear issue via `mcp__linear__save_issue` before re-posting.
4. Re-post revised plan to Linear and re-enter plan mode.

---

## 5) Implementation

### XS/S fast path

When ALL are true: estimate is XS or S (1-2 points), changes are isolated to one area. User can override in either direction.

- Implement directly (single agent).
- Handle tests and logging inline.
- Run `cd app && npx tsc --noEmit` before committing.
- Only the **code reviewed** and **final validation** quality gates apply.
- Code review and PR creation are never skipped.

### M+ or multi-component

Execute the approved plan. Use parallelism where work streams are independent.

Team lead rules for each work stream:

- **Test deliberately.** Read existing tests first. Update broken tests, remove redundant ones, add tests only for new behavior.
- Commit + push before reporting (conventional commit format).
- Check off owned ACs on Linear after tests pass.
- Report: what completed, tests updated/added/removed, ACs addressed, blockers.
- Max 2 retries per stream before escalating to user. Pause dependent streams if a blocking failure occurs.

---

## 6) Branch Sync (required)

Before running quality gates, rebase onto `origin/main`.

1. Fetch latest `origin/main`.
2. Rebase current branch onto `origin/main`.
3. Resolve conflicts when mechanical; escalate to user when semantic.
4. Push with `--force-with-lease` if history changed.

---

## 7) Quality Gates

```text
- [ ] Tests written
- [ ] Tests passing
- [ ] Logging compliant
- [ ] Brand compliant
- [ ] Code simplified (large diffs only)
- [ ] Code reviewed
- [ ] Docs updated
- [ ] Final validation
```

For XS/S fast path, only **code reviewed** and **final validation** are required — the single agent handles the rest inline.

### Tests written

Add targeted tests for changed behavior only. Update/remove obsolete tests.

### Tests passing

1. `cd app && npx tsc --noEmit`
2. Test commands based on changed areas per repo guidelines.

### Logging compliant

Confirm changed code follows repo logging rules.

### Brand compliant

Run the repo's off-brand color grep check for changed frontend files.

### Code simplified (optional)

Required only when diff is large (roughly >5 files or >300 LOC) or readability is clearly degraded.

### Code reviewed

Run a focused code review pass. Fix cycle rules:

- **High/medium severity**: must fix.
- **Low severity**: fix if straightforward, otherwise note.
- **Max 2 review cycles**, then proceed with remaining low-severity notes.

### Docs updated

Update docs only where behavior/commands/conventions changed.

### Final validation

Run final relevant tests after fixes/review.

---

## 8) Completion

### Update Linear with implementation results

Post an implementation notes comment to Linear via `mcp__linear__save_comment` covering:

- **Status**: Ready for Review
- **Branch** and **PR** URL
- **What was done**: brief list of completed work
- **Tests**: what was tested and results

Update the issue description's Implementation Updates section via `mcp__linear__save_issue`.

### Verify AC coverage

- Evaluate every issue checkbox (Scope / Requirements / AC / Test Notes).
- Check only items demonstrably implemented in code/tests.
- Leave unverifiable or partial items unchecked.
- Update checkboxes on Linear via `mcp__linear__save_issue`.
- Add one concise comment with evidence for each newly checked item.

### Create or update PR

- Use `gh pr create` or `gh pr edit` with `--body-file <tmp.md>`.
- PR body format: see [`references/git-and-pr.md`](references/git-and-pr.md).
- PR body must include `Fixes <issue-id>` lines for primary and child issues.
- Verify with `gh pr view --json url,number,body,state,headRefName,baseRefName,statusCheckRollup`.

### Branch protection

- Inspect required checks for the base branch.
- If required checks exist: ensure PR state/checks satisfy them.
- If none: post risk comments on PR and Linear stating no required checks.

### Move to In Review

Move issue(s) to `In Review` via `mcp__linear__save_issue`.

### Report

Return: PR URL, worktree path, recommended test mode (see [`references/test-mode.md`](references/test-mode.md)), and manual test steps.

Do not remove the worktree — the user tests manually on it.

---

## 9) Error Recovery

| Situation | Action |
|---|---|
| Worktree exists on wrong branch | Remove and recreate |
| Linear API fails | Retry once, then stop with details |
| Tests fail after 3 attempts | Escalate with failure details |
| ACs remain unmet | Keep `In Progress` and report gaps |
| Plan rejected twice | Ask user for explicit direction |

---

## References

- [`references/git-and-pr.md`](references/git-and-pr.md) — PR body template, test plan guidelines, worktree rules
- [`references/test-mode.md`](references/test-mode.md) — mock vs full mode decision rule
