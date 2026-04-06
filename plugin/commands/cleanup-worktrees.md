---
name: cleanup-worktrees
description: >
  Scan git worktrees for branches with merged PRs and clean them up
  (remove worktree, delete local and remote branches). Then sweep
  gone branches (remote deleted) that have merged PRs.
user-invocable: true
argument-hint: "[branch-name]"
---

# Cleanup Worktrees

Remove worktrees whose PRs have been merged. Can target a single branch or scan all worktrees. After the PR-based cleanup, sweeps local branches whose remote is gone.

## Step 1 — Discover worktrees

If `$ARGUMENTS` contains a branch name, target only that branch. Otherwise scan all:

```bash
git worktree list --porcelain
```

Parse the output to build a list of worktrees (excluding the main working tree). For each worktree, extract the branch name.

## Step 2 — Check PR status

For each worktree branch, check whether a merged PR exists:

```bash
gh pr list --head <branch> --state merged --json number,title,url --jq '.[0]'
```

Classify each worktree:

| PR state | Action |
|---|---|
| Merged PR found | Queue for cleanup |
| Open PR found | Skip — report as "PR still open" |
| No PR found | Skip — report as "no PR found" |

## Step 3 — Clean up merged worktrees

For each worktree queued for cleanup:

1. `git worktree remove <worktree-path>`
2. `git branch -d <branch>` (safe delete — will fail if not fully merged, which is correct)
3. `git push origin --delete <branch>` (delete remote branch; ignore errors if already deleted)

## Step 4 — Sweep gone branches

After the PR-based cleanup, identify local branches whose remote tracking ref is gone:

```bash
git fetch --prune
git branch -v
```

For each branch that shows `[gone]` in `git branch -v` output:

1. Check whether a merged PR exists:

```bash
gh pr list --head <branch> --state merged --json number,title,url --jq '.[0]'
```

1. If a merged PR is found: remove the associated worktree if one exists, then force-delete the local branch:

```bash
git worktree remove <worktree-path>   # only if worktree exists for this branch
git branch -D <branch>
```

1. If no PR found or PR is open: skip and record as skipped.

Skip branches that were already handled in Step 3.

## Step 5 — Report

Present a combined summary with two sections:

```text
cleanup-worktrees complete

Worktree cleanup:
  ✓ feature/scope-silver-dimcustomer    cleaned (PR #42 merged)
  - feature/profile-silver-dimcustomer  skipped (PR #45 still open)
  - feature/generate-model-old          skipped (no PR found)

  cleaned: 1 | skipped: 2

Gone branch sweep:
  ✓ feature/vu-870-old-feature          cleaned (PR #38 merged)
  - feature/vu-871-wip                  skipped (no PR found)

  cleaned: 1 | skipped: 1
```

If no gone branches were found, omit the "Gone branch sweep" section.

## Error handling

| Condition | Action |
|---|---|
| `git worktree remove` fails | Report error, continue to next worktree |
| `git branch -d` fails | Branch not fully merged — report warning, do not force delete |
| `git branch -D` fails (gone branch) | Report error, continue |
| `git push origin --delete` fails | Remote branch already gone — ignore |
| No worktrees found | Tell user there are no worktrees to clean up |
