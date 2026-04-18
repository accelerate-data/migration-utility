---
name: generating-tests
description: >
  Use when a migrated table, view, or materialized view needs a new or
  merge-safe `test-specs/<schema.object>.json`, especially after SQL changes or
  reviewer feedback.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Tests

Generate or extend a `TestSpec` for one migrated object.

## When To Use

Use this skill when:

- `migrate-util ready test-gen --object <fqn>` should pass and the next artifact is `test-specs/<fqn>.json`
- an existing spec needs merge-safe updates after SQL changed
- `reviewing-tests` returned `feedback_for_generator`
- the target object is a table, view, or materialized view and fixtures must follow catalog metadata

Do not use this skill to review coverage, run sandbox execution, or generate dbt SQL.

## Quick Reference

| Step | Required action |
|---|---|
| Guard | Ready check must pass before any write |
| Type | Detect `table`, `view`, or `mv` from catalog |
| Invariants | Apply [../test-invariants/SKILL.md](../test-invariants/SKILL.md) |
| Merge mode | Preserve approved scenarios and `expect` blocks |
| Feedback | On repair passes, follow reviewer feedback before broad regeneration |
| Guard rails | Use [references/guard-rails-ref.md](references/guard-rails-ref.md) when merge, feedback, or coverage judgment is ambiguous |
| Output | Write a valid `TestSpec`, then persist the catalog summary |

## Implementation

1. Run the stage guard first:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util ready test-gen \
     --object <target_fqn> \
     --project-root <project_root>
   ```

   If readiness fails, report the surfaced `code` and `reason` values verbatim and stop. Do not inspect SQL, generate fixtures, or write test specs after a failed readiness check.
2. Detect object type from catalog before generating: absent view catalog means `table`; `is_materialized_view: true` means `mv`; otherwise `view`.
3. If `test-specs/<fqn>.json` exists, enter merge mode and load existing scenario names, `branch_manifest`, and `expect` blocks. Also load deterministic object and source-table context from catalog metadata.
4. Re-extract branches from current SQL. For tables, current SQL is `selected_writer_ddl_slice` when present, otherwise `proc_body`; if both are empty, stop with a context error. Build a fresh `branch_manifest`; if a prior branch disappeared, add a `STALE_BRANCH` warning.
5. Generate only missing or explicitly requested coverage. On reviewer-driven reruns, follow the repair-pass rules below. Keep scenarios self-contained and constraint-valid; do not invent columns, nullability, or FK behavior.
6. Emit a valid `TestSpec`, preserving approved scenarios unless targeted feedback requires revision. Recalculate `uncovered_branches`, `coverage`, and `status`, then persist the catalog summary.

## Handling Reviewer Feedback

When `reviewing-tests` returns `feedback_for_generator`, apply [../test-invariants/SKILL.md](../test-invariants/SKILL.md) and start from the existing spec.

Repair-pass algorithm:

1. Load the existing spec.
2. Keep every existing `unit_tests[]` entry unless a `quality_fixes` instruction names that scenario.
3. Keep every unrelated branch's `scenarios[]` list unchanged.
4. Add or edit only the scenarios needed for the requested `uncovered_branches` and named `quality_fixes`.
5. Recalculate `uncovered_branches`, `coverage`, and `status` from the merged result.

Use the exact command flow from [references/command-workflow-ref.md](references/command-workflow-ref.md), the runtime field checklist from [references/test-spec-contract-ref.md](references/test-spec-contract-ref.md), the fixture guidance from [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md), and the expanded guard rails from [references/guard-rails-ref.md](references/guard-rails-ref.md).

## Common Mistakes

- Treating stored specs or branch manifests as authority instead of current SQL and catalog state.
- Treating all `catalog/views/*.json` entries as `view` instead of detecting `mv`.
- Rewriting approved scenarios or schema shape when only incremental coverage is needed.

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
