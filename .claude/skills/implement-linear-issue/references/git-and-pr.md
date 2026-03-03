# Git and PR Conventions

## PR Body Template

Title: `<issue-id>: short description`

```markdown
Fixes <issue-id>

<!-- If the PR covers child issues, list EACH on its own line: -->
<!-- Fixes ABC-530 -->
<!-- Fixes ABC-531 -->
<!-- Fixes ABC-532 -->
<!-- NEVER group as "Fixes ABC-530/531/532" — each must be a separate line. -->

## Summary
[2-3 sentences from implementation status]

## Changes
- [Bullet list from team reports]

## Test plan
- [x] [Automated tests that passed, with counts]
- [ ] [Manual verification step 1]
- [ ] [Manual verification step 2]
- [ ] [... one checkbox per user-facing behavior to verify]

## Acceptance Criteria
- [x] [AC 1]
- [x] [AC 2]
```

## Test Plan Guidelines

The test plan section is **checked during `/close-issue`** — unchecked items block the merge.

- **Automated tests**: Mark `[x]` immediately after tests pass. Include test counts.
- **Manual tests**: Leave `[ ]` unchecked. The user checks these off on the PR after manual testing.
- Write manual test items as concrete steps the user can follow (action → expected result).
- Cover every user-visible behavior change — not internals.

After creating the PR, update the Linear issue via `mcp__linear__save_issue` and/or `mcp__linear__create_comment` with the PR URL.

## Worktree Preservation

**Do NOT remove the worktree.** The user tests manually on it. Include the worktree path in the final status report.
