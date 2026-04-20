# Whole-Mart Migration

Use this flow when you want the plugin to migrate a scoped mart end to end.

## Flow

| Step | Command | What happens |
|---|---|---|
| 1 | `/init-ad-migration` | Initialize the migration repo and choose source and target technology |
| 2 | `ad-migration setup-source` | Extract DDL and build the catalog |
| 3 | `/scope-tables` | Discover writer procedures and table or view migration scope |
| 4 | `ad-migration add-source-table`, `ad-migration add-seed-table`, `ad-migration exclude-table` | Classify source, seed, and excluded objects |
| 5 | `ad-migration setup-target` | Configure and verify target and dbt runtime state required for planning |
| 6 | `ad-migration setup-sandbox` | Configure and verify sandbox runtime state required for planning |
| 7 | `/migrate-mart-plan` | Verify readiness, write or refresh the mart plan, and open the planning PR |
| 8 | Review planning PR | Approve the plan or revise catalog decisions |
| 9 | `/migrate-mart <plan-file>` | Execute the first incomplete plan stage until the final coordinator PR |

## How it works

`/migrate-mart-plan` reads the catalog after scoping and source, seed, and excluded-object decisions. It checks that source, target, sandbox, and dbt project state are ready, then writes a resumable Markdown plan.

`/migrate-mart` consumes the approved plan and coordinates the same stage commands you can run manually:

- `/profile-tables`
- `/generate-tests`
- `/refactor-query`
- `ad-migration replicate-source-tables`
- `/generate-model`
- `/refactor-mart`

It runs one stage at a time, uses isolated worktrees, merges stage PRs into the coordinator branch, updates the plan, and leaves the final coordinator PR for human review.

## Human decision points

After scoping, decide which objects are sources, seeds, or excluded from the migration.

After planning, review the planning PR. If source, seed, or excluded-object decisions change, update the catalog and rerun `/migrate-mart-plan`.

After execution, review and merge the final coordinator PR.

## Reruns and recovery

Before plan approval, change catalog decisions and rerun `/migrate-mart-plan`.

During execution, rerun `/migrate-mart <plan-file>`. It resumes from the first incomplete stage and reuses the recorded branch and worktree state.

If a stage is blocked, fix the blocker and rerun the same command.

## Related pages

- [[Command Migrate Mart Plan]]
- [[Command Migrate Mart]]
- [[Scoping]]
- [[Profiling]]
- [[Test Generation]]
- [[SQL Refactoring]]
- [[Model Generation]]
