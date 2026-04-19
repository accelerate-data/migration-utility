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

In Claude Code slash commands, `$0` is the first user-supplied argument.
Manual mode is active only when the positional arguments are exactly:

- `$0 = <refactor-mart-plan-file>`
- `$1 = stg|int`

Coordinator mode is active only when the positional arguments are exactly:

- `$0 = <migrate-mart-plan-file>`
- `$1 = <stage-id>`
- `$2 = <worktree-name>`
- `$3 = <base-branch>`
- `$4 = <refactor-mart-plan-file>`
- `$5 = stg|int`

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
2. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example `1743868200123-a1b2c3d4`). Use it as the suffix for every artifact written by this run, including `.migration-runs/pr-body.<run_id>.md`.
3. Use the `## Arguments` contract above to determine whether this is manual mode or coordinator mode. Do not infer coordinator mode from `$0` alone; the full positional shape must match exactly.
4. Use `${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh` for setup instead of `git-checkpoints`.
   - Coordinator mode: read `Branch:`, `Worktree name:`, and `Base branch:` from the matching stage section in the migrate-mart plan, then run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh" "<branch>" "<worktree-name>" "<base-branch>"
     ```

     Use the returned `worktree_path` for all reads, writes, commits, and prompts.
   - Manual mode: derive a stable branch name from the run slug, resolve the remote default branch, and call the same helper with those explicit values.
5. In coordinator mode, own only the matching `## Stage <stage-id>` checklist in `<migrate-mart-plan-file>`. After each stage substep or candidate result, update only that checklist, then commit the plan update together with the artifact or catalog change that caused it.
6. Read the coordinator stage metadata from `$0` and the nested refactor-mart plan file from `$4` in that working directory.

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

If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."
