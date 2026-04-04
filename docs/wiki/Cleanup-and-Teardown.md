# Cleanup and Teardown

Two cleanup commands remove resources created during the migration pipeline: `/cleanup-worktrees` for git worktrees and `/teardown-sandbox` for the throwaway test database.

## `/teardown-sandbox`

Drops the throwaway sandbox database (`__test_<run_id>`) that was created by `/setup-sandbox` during test generation.

### When to run

Run `/teardown-sandbox` after test generation is complete for all tables in the current batch. The sandbox is only needed while `/generate-tests` or `/generating-tests` is actively executing stored procedures to capture ground truth. Once all test specs are written to `test-specs/`, the sandbox can be safely dropped.

### How it works

1. Reads `manifest.json` to find the `sandbox.run_id` and `sandbox.database` fields
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

Scans git worktrees for branches with merged PRs and removes them. Can target a single branch or scan all worktrees.

### When to run

Run `/cleanup-worktrees` after PRs have been merged. Batch commands create a worktree per invocation, and these accumulate over time. Periodic cleanup keeps the workspace tidy.

### Usage

- **All worktrees:** `/cleanup-worktrees` -- scans every worktree except the main working tree
- **Single branch:** `/cleanup-worktrees <branch-name>` -- targets only the specified branch

### How it works

1. Lists all worktrees via `git worktree list --porcelain`
2. For each worktree branch, checks GitHub for a merged PR via `gh pr list --head <branch> --state merged`
3. Classifies each worktree:
   - **Merged PR found** -- queued for cleanup
   - **Open PR found** -- skipped ("PR still open")
   - **No PR found** -- skipped ("no PR found")
4. For each worktree queued for cleanup:
   - Removes the worktree (`git worktree remove`)
   - Deletes the local branch (`git branch -d` -- safe delete, fails if not fully merged)
   - Deletes the remote branch (`git push origin --delete` -- ignores errors if already deleted)
5. Presents a summary:

```text
cleanup-worktrees complete

  > feature/scope-silver-dimcustomer    cleaned (PR #42 merged)
  - feature/profile-silver-dimcustomer  skipped (PR #45 still open)
  - feature/generate-model-old          skipped (no PR found)

  cleaned: 1 | skipped: 2
```

### Error handling

| Situation | Behavior |
|---|---|
| `git worktree remove` fails | Reports error, continues to next worktree |
| `git branch -d` fails | Branch not fully merged -- reports warning, does not force delete |
| `git push origin --delete` fails | Remote branch already gone -- ignored |
| No worktrees found | Reports that there are no worktrees to clean up |

## Related pages

- [[Git Workflow]] -- how worktrees and branches are created
- [[Stage 6 Test Generation]] -- when the sandbox is used
