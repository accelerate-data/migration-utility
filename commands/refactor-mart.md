---
name: refactor-mart
description: Use when applying approved candidates from a refactor-mart markdown plan
user-invocable: true
argument-hint: "<plan-file> stg|int"
---

# Refactor Mart

Apply one approved wave from a `/refactor-mart-plan` markdown plan.

Modes:

- `stg`: approved `Type: stg` candidates only.
- `int`: approved `Type: int` and `Type: mart` candidates only.

Never apply unapproved candidates. Never edit candidates outside the selected mode. `skipped` is a final-summary category only; do not write `Execution status: skipped`.

## Guards

- Parse exactly two positional arguments: `<plan-file>` and `stg|int`.
- If the plan file is missing, fail with `PLAN_NOT_FOUND`.
- If mode is not `stg` or `int`, fail with `INVALID_MODE`.
- If `manifest.json` is missing, fail with `MANIFEST_NOT_FOUND`.
- If `dbt/dbt_project.yml` is missing, fail with `DBT_PROJECT_MISSING` and tell the user to run `ad-migration setup-target`.

## Setup

1. Generate run slug `refactor-mart-<mode>-<plan-stem>`.
2. Run `git-checkpoints` with the run slug.
3. Use the returned worktree path for reads, writes, commits, and plan updates.
   If it returns the default branch name, use the current repository root.
4. Read the plan in that working directory.

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
