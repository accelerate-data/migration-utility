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

Generate or extend a `TestSpec` for one migrated object. This skill is responsible for producing a valid spec file and recording the test-gen summary in catalog state. It does not execute routines, capture ground truth, or review its own work.

## When To Use

Use this skill when:

- `migrate-util ready <fqn> test-gen` should pass and the next artifact is `test-specs/<fqn>.json`
- an existing spec needs merge-safe updates after SQL changed
- `reviewing-tests` returned `feedback_for_generator`
- the target object is a table, view, or materialized view and fixtures must match catalog-backed columns

Do not use this skill to review coverage, run sandbox execution, or generate dbt SQL.

## Quick Reference

| Step | Required action |
|---|---|
| Guard | Run `migrate-util ready <fqn> test-gen`; stop on failure |
| Object type | Read `catalog/views/<fqn>.json`; if present, inspect `is_materialized_view` to choose `view` vs `mv`; otherwise `table` |
| Context | Tables: `migrate context --table <fqn> [--writer ...]`; Views/MVs: `discover show --name <fqn>` plus view catalog |
| Branches | Re-extract from SQL using `../_shared/references/branch-patterns.md`; never trust stored branch manifests |
| Merge mode | If `test-specs/<fqn>.json` exists, preserve existing scenarios and `expect` blocks; add only new or requested coverage |
| Validation | Emit a valid `TestSpec`; if summary write fails, fix the spec and retry |
| Catalog write | Run `test-harness write --table <fqn> --branches ... --unit-tests ... --coverage ...` |

## Implementation

### 1. Guard and detect object type

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> test-gen
```

If readiness fails, report the surfaced `code` and `message` and stop.

Detect object type from catalog:

- `catalog/views/<fqn>.json` absent -> `object_type = table`
- `catalog/views/<fqn>.json` present with `is_materialized_view: true` -> `object_type = mv`
- `catalog/views/<fqn>.json` present otherwise -> `object_type = view`

### 2. Load prior spec state before generating anything

If `test-specs/<fqn>.json` exists, set `merge_mode = true` and load:

- `unit_tests[].name`
- `branch_manifest`
- existing `expect` blocks keyed by scenario name

If the file does not exist, set `merge_mode = false`.

### 3. Assemble deterministic context

For tables:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context --table <fqn>
```

Use the selected writer from catalog scoping when the command output does not already provide it.

For views and materialized views:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <fqn>
```

Also read `catalog/views/<fqn>.json` for:

- `profile`
- `scoping.logic_summary`
- `references.tables.in_scope`
- `is_materialized_view`

For all source tables, read `catalog/tables/<schema>.<table>.json` to get column nullability, identity, types, foreign keys, and `auto_increment_columns`.

### 4. Re-extract branches from current SQL

Enumerate branches from the current procedure/view SQL using `../_shared/references/branch-patterns.md`.

Rules:

- build a fresh `branch_manifest`; do not trust or copy the stored manifest
- include stable branch IDs, `statement_index`, and descriptions
- if `merge_mode` and a previously stored branch disappears, append a `STALE_BRANCH` warning

### 5. Generate only the coverage that is actually missing

If `feedback_for_generator` is present, apply it first:

- `uncovered_branches`: generate scenarios for those branch IDs
- `quality_fixes`: revise only the named scenarios

Without feedback:

- first run: generate scenarios for all branches
- merge mode: generate only for uncovered branches after comparing the new manifest to existing scenarios

Fixture rules:

- each scenario is self-contained
- use type-appropriate values from catalog metadata
- every `given[].rows` entry includes all NOT NULL non-identity columns for that source table
- table scenarios may need FK-consistent row graphs
- view and mv scenarios use `sql`; table scenarios use `target_table` and `procedure`

Apply detailed synthesis rules from [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md).

### 6. Write a valid `TestSpec`

Emit a `TestSpec` matching the runtime contract in `shared.output_models.test_specs.TestSpec`.

Required fields:

- `item_id`
- `object_type` in `table | view | mv`
- `status` in `ok | partial | error`
- `coverage` in `complete | partial`
- `branch_manifest`
- `unit_tests`
- `uncovered_branches`
- `validation`
- `warnings`
- `errors`

Merge mode rules:

- append new `unit_tests[]`; do not overwrite existing tests unless a `quality_fixes` instruction explicitly targets that scenario
- preserve existing `expect` blocks unless revising that scenario is required
- add new branches to `branch_manifest[]`; extend `scenarios[]` for existing branches
- recalculate `uncovered_branches`, `coverage`, and `status` from the merged result

Coverage/status ownership:

- this skill sets `coverage` and `status` on the generated spec
- `reviewing-tests` independently audits coverage and may approve, warn, or kick back the result

### 7. Persist the summary to catalog

After the spec is written successfully, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness write \
  --table <fqn> \
  --branches <branch_count> \
  --unit-tests <scenario_count> \
  --coverage <complete|partial|none>
```

Pass warnings/errors as JSON arrays when present. If the write command rejects the artifact, fix the spec and retry.

## Common Mistakes

- Skipping branch re-extraction in merge mode. Existing specs are hints, not authority.
- Treating all `catalog/views/*.json` entries as `view`. Materialized views must emit `object_type = mv`.
- Inventing schema fields not present in the runtime `TestSpec` model.
- Regenerating or deleting approved scenarios when only incremental coverage is needed.
- Treating reviewer feedback as optional. `feedback_for_generator` is a direct instruction to revise or extend the spec.

## Rationalization Checks

| Rationalization | Correct behavior |
|---|---|
| "The old branch manifest is close enough." | Re-extract from current SQL and warn on stale branches. |
| "Merge mode means I can rewrite the whole file." | Preserve existing scenarios and `expect` blocks unless feedback requires a targeted revision. |
| "Materialized views behave like views, so `view` is fine." | Detect `is_materialized_view` and emit `mv`. |
| "Reviewer coverage is the real score, so this spec field does not matter." | The spec must still carry valid `coverage` and `status` fields. |

## Boundary Rules

- Write only `test-specs/<fqn>.json` plus the catalog summary written through `test-harness write`
- Do not execute routines or sandbox scenarios
- Do not review your own coverage quality
- Do not generate dbt SQL or change profile/scoping decisions

## References

- [references/fixture-synthesis-ref.md](references/fixture-synthesis-ref.md)
- [../_shared/references/branch-patterns.md](../_shared/references/branch-patterns.md)
- [../../lib/shared/generate_tests_error_codes.md](../../lib/shared/generate_tests_error_codes.md)
