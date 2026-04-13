# Cleanup and Teardown

Two cleanup commands remove resources created during the migration pipeline: `/cleanup-worktrees` for git worktrees and branches, and `/teardown-sandbox` for the throwaway test database.

## `/teardown-sandbox`

Drops the active sandbox endpoint that was created by `/setup-sandbox` during test generation.

### When to run

Run `/teardown-sandbox` after test generation is complete for all tables in the current batch. The sandbox is only needed while `/generate-tests` or `/generating-tests` is actively executing stored procedures to capture ground truth. Once all test specs are written to `test-specs/`, the sandbox can be safely dropped.

### How it works

1. Reads `manifest.json` to find the `runtime.sandbox` field
2. Shows the user which database will be dropped and asks for confirmation (this is a destructive operation)
3. Runs `test-harness sandbox-down` to drop the database
4. Clears the `sandbox` section from `manifest.json`

### Safety

- The command requires explicit user confirmation before dropping the database
- If the database has already been dropped (e.g. manually), the command reports success rather than an error
- If no sandbox metadata exists in `manifest.json`, the command stops and tells the user no sandbox exists

### Error handling

| Situation | Behavior |
|---|---|
| Drop succeeds | Sandbox section cleared from manifest, success reported |
| Database already gone | Reported as success, not an error |
| Connection error or permissions failure | Reports `SANDBOX_DOWN_FAILED` error with details |
| No sandbox metadata in manifest | Stops with message that no sandbox exists |

## `/cleanup-worktrees`

Scans git worktrees and local branches for merged PRs and removes them. Runs three passes in sequence.

### When to run

Run `/cleanup-worktrees` after PRs have been merged. Batch commands create a worktree per invocation, and these accumulate over time. Periodic cleanup keeps the workspace tidy.

### Usage

- **All worktrees:** `/cleanup-worktrees` -- scans every worktree except the main working tree
- **Single branch:** `/cleanup-worktrees <branch-name>` -- targets only the specified branch

### How it works

**Pass 1 — Worktree cleanup**

1. Lists all worktrees via `git worktree list --porcelain`
2. For each worktree branch, checks GitHub for a merged PR via `gh pr list --head <branch> --state merged`
3. Classifies each worktree:
   - **Merged PR found** — remove worktree, delete local branch (`git branch -d`), delete remote branch (`git push origin --delete`)
   - **Open PR found** — skipped ("PR still open")
   - **No PR found** — skipped ("no PR found")

**Pass 2 — Gone branch sweep**

After the worktree cleanup, runs `git fetch --prune` then scans `git branch -v` for branches marked `[gone]` (remote tracking ref no longer exists). For each gone branch:

- If a merged PR exists: removes any associated worktree, then force-deletes the local branch (`git branch -D`)
- If no PR or open PR: skipped and reported

Branches already handled in Pass 1 are skipped.

**Pass 3 — Remote-only sweep**

Scans `git branch -r` for remote branches that have no local counterpart. For each:

- If a merged PR exists: deletes the remote branch (`git push origin --delete`)
- If no PR: skipped ("no PR found")
- If open PR: skipped ("PR still open")

Skips `origin/HEAD`, `origin/main`, and branches handled in Passes 1 or 2.

### Summary

The command presents results in three separate sections:

```text
cleanup-worktrees complete

Worktree cleanup:
  ✓ feature/scope-silver-dimcustomer    cleaned (PR #42 merged)
  - feature/profile-silver-dimcustomer  skipped (PR #45 still open)
  - feature/generate-model-old          skipped (no PR found)

  cleaned: 1 | skipped: 2

Gone branch sweep:
  ✓ feature/refactor-silver-dimcustomer  cleaned (PR #43 merged)
  - feature/wip-branch                   skipped (no PR found)

  cleaned: 1 | skipped: 1

Remote-only sweep:
  ✓ feature/scope-silver-dimdate         cleaned (PR #41 merged)
  - feature/open-pr-branch               skipped (PR still open)

  cleaned: 1 | skipped: 1
```

Sections with no results are omitted from the output.

### Error handling

| Situation | Behavior |
|---|---|
| `git worktree remove` fails | Reports error, continues to next worktree |
| `git branch -d` fails | Branch not fully merged — reports warning, does not force delete |
| `git branch -D` fails (gone branch) | Reports error, continues |
| `git push origin --delete` fails | Reports error, continues |
| No worktrees found | Reports that there are no worktrees to clean up |

## Related pages

- [[Git Workflow]] -- how worktrees and branches are created
- [[Stage 3 Test Generation]] -- when the sandbox is used
