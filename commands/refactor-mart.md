---
name: refactor-mart
description: Use when applying approved candidates from a refactor-mart markdown plan
user-invocable: true
argument-hint: "<plan-file> stg|int"
---

# Refactor Mart

Apply one approved wave from a `/refactor-mart-plan` markdown plan.

## Arguments

Manual mode:

```text
/refactor-mart <plan-file> stg|int
```

Coordinator mode:

```text
/refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg|int
```

Coordinator mode is active only when `$0` is a Markdown plan path.

Modes:

- `stg`: approved `Type: stg` candidates only.
- `int`: approved `Type: int` and `Type: mart` candidates only.

Never apply unapproved candidates. Never edit candidates outside the selected mode. `skipped` is a final-summary category only; do not write `Execution status: skipped`.

## Guards

- If the plan file is missing, fail with `PLAN_NOT_FOUND`.
- If mode is not `stg` or `int`, fail with `INVALID_MODE`.
- If `manifest.json` is missing, fail with `MANIFEST_NOT_FOUND`.
- If `dbt/dbt_project.yml` is missing, fail with `DBT_PROJECT_MISSING` and tell the user to run `ad-migration setup-target`.

## Setup

1. Generate run slug `refactor-mart-<mode>-<plan-stem>`.
2. Use the `## Arguments` contract above to determine whether this is manual mode or coordinator mode.
3. Use `${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh` for setup instead of `git-checkpoints`.
   - Coordinator mode: read `Branch:`, `Worktree name:`, and `Base branch:` from the matching stage section in the migrate-mart plan, then run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh" "<branch>" "<worktree-name>" "<base-branch>"
     ```

     Use the returned `worktree_path` for all reads, writes, commits, and prompts.
   - Manual mode: derive a stable branch name from the run slug, resolve the remote default branch, and call the same helper with those explicit values.
4. In coordinator mode, own only the matching `## Stage <stage-id>` checklist in `<migrate-mart-plan-file>`. After each stage substep or candidate result, update only that checklist, then commit the plan update together with the artifact or catalog change that caused it.
5. Read the refactor-mart plan file from `$4` in that working directory.

## Candidate Selection

Select only sections with exact checked approval syntax:
`- [x] Approve: yes`.

| Mode | Select | Leave unchanged |
| --- | --- | --- |
| `stg` | `Type: stg` | `Type: int`, `Type: mart`, unapproved candidates |
| `int` | `Type: int`, `Type: mart` | `Type: stg`, unapproved candidates |

## Dependency Gate For `int`

Check dependencies before any dbt edit or apply-skill invocation.

Block the selected candidate in the plan when:

- `Depends on:` is missing, empty, malformed, ambiguous, or not exactly `none` or candidate IDs;
- a referenced dependency section is missing; or
- any dependency is not `Execution status: applied`.

Include the metadata problem, or each missing/unsatisfied dependency ID and its actual status, in exactly one `Blocked reason:` bullet.

Process selected candidates in plan order. After each apply-skill invocation, reread the plan before gating the next selected candidate so a newly applied candidate can satisfy downstream dependencies in the same wave.

After the guards pass, perform selected plan and dbt edits directly in the active working directory. Do not ask for permission before dispatching a selected candidate.

## Apply

| Mode | Dispatch |
| --- | --- |
| `stg` | `applying-staging-candidate <plan-file> <candidate-id>` |
| `int` | `applying-mart-candidates <plan-file> <candidate-id>` |

Ownership:

| Owner | Writes |
| --- | --- |
| `/refactor-mart` | dependency-blocked candidate statuses and reasons |
| apply skill | dbt edits, candidate-scoped validation, non-dependency input blocks |

## Summary

After all selected candidates are processed, reread the plan and report:

```text
refactor-mart <mode> complete -- <plan-file>

applied: <n>
failed: <n>
blocked: <n>
skipped: <n>
```

List blocked candidate IDs with dependency or metadata reasons. List failed candidate IDs with validation summaries. Skipped means approved candidates not selected for the current mode plus unapproved candidates in the current wave; blocked candidates are not skipped.

After successful candidate work is committed and pushed, always open or update a PR:

```bash
"${CLAUDE_PLUGIN_ROOT}/shared/scripts/stage-pr.sh" "<branch>" "<base-branch>" "<title>" ".migration-runs/pr-body.<run_id>.md"
```

Report the PR number and URL. In manual mode, tell the human to review and merge the PR. In coordinator mode, return the PR metadata to the coordinator and do not ask any question.
