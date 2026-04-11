---
name: generating-tests
description: >
  Use when generating or extending `test-specs/<schema.object>.json` for a
  migrated table, view, or materialized view after scoping/profile are ready
  and the next step depends on branch coverage, merge-safe updates, or
  reviewer-driven fixture fixes.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Tests

Generate or extend a `TestSpec` for one migrated object. Core rule: regenerate coverage from current SQL and merge safely with approved tests instead of rewriting from memory.

## When To Use

Use this skill when:

- `migrate-util ready <fqn> test-gen` should pass and the next artifact is `test-specs/<fqn>.json`
- an existing spec needs merge-safe updates after SQL changed
- `reviewing-tests` returned `feedback_for_generator`
- the target object is a table, view, or materialized view and fixtures must follow catalog metadata

Do not use this skill to review coverage, run sandbox execution, or generate dbt SQL.

## Quick Reference

| Step | Required action |
|---|---|
| Guard | Ready check must pass before any write |
| Type | Detect `table`, `view`, or `mv` from catalog |
| Branches | Re-extract current `branch_manifest`; never trust the stored one |
| Merge mode | Preserve approved scenarios and `expect` blocks |
| Feedback | `feedback_for_generator` overrides broad regeneration |
| Guard rails | Use [references/guard-rails-ref.md](references/guard-rails-ref.md) when merge, feedback, or coverage judgment is ambiguous |
| Output | Write a valid `TestSpec`, then persist the catalog summary |

## Implementation

1. Run the stage guard first. If readiness fails, report the surfaced `code` and `message` and stop.
2. Detect object type from catalog before generating: absent view catalog means `table`; `is_materialized_view: true` means `mv`; otherwise `view`.
3. If `test-specs/<fqn>.json` exists, enter merge mode and load existing scenario names, `branch_manifest`, and `expect` blocks. Also load deterministic object and source-table context from catalog metadata.
4. Re-extract branches from current SQL. Build a fresh `branch_manifest`; if a prior branch disappeared, add a `STALE_BRANCH` warning.
5. Generate only missing or explicitly requested coverage. `feedback_for_generator` takes priority. Keep scenarios self-contained and constraint-valid; do not invent columns, nullability, or FK behavior.
6. Emit a valid `TestSpec`, preserving approved scenarios unless targeted feedback requires revision. Recalculate `uncovered_branches`, `coverage`, and `status`, then persist the catalog summary.

Use the exact command flow from [references/command-workflow-ref.md](references/command-workflow-ref.md), the runtime field checklist from [references/test-spec-contract-ref.md](references/test-spec-contract-ref.md), the fixture guidance from [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md), and the expanded guard rails from [references/guard-rails-ref.md](references/guard-rails-ref.md).

## Common Mistakes

- Treating stored specs or branch manifests as authority instead of current SQL and catalog state.
- Treating all `catalog/views/*.json` entries as `view` instead of detecting `mv`.
- Rewriting approved scenarios or schema shape when only incremental coverage is needed.

## Rationalization Checks

| Rationalization | Correct behavior |
|---|---|
| "The old branch manifest is close enough." | Re-extract from current SQL and warn on stale branches. |
| "Merge mode means I can rewrite the whole file." | Preserve existing scenarios unless feedback requires a targeted revision. |
| "`feedback_for_generator` is advisory." | Apply requested branch and quality-fix work before broad generation. |

## Boundary Rules

- Write only `test-specs/<fqn>.json` plus the catalog summary written through `test-harness write`
- Do not execute routines or sandbox scenarios
- Do not review your own coverage quality
- Do not generate dbt SQL or change profile/scoping decisions
- Do not overwrite valid existing tests unless targeted feedback requires it

## References

- [references/command-workflow-ref.md](references/command-workflow-ref.md)
- [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md)
- [references/guard-rails-ref.md](references/guard-rails-ref.md)
- [references/test-spec-contract-ref.md](references/test-spec-contract-ref.md)
- [../_shared/references/branch-patterns.md](../_shared/references/branch-patterns.md)
- [../../lib/shared/generate_tests_error_codes.md](../../lib/shared/generate_tests_error_codes.md)
