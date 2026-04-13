# TestSpec Contract Reference

Use this checklist when validating the generated artifact against `shared.output_models.test_specs.TestSpec`.

## Required fields

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

## Merge mode rules

- Append new `unit_tests[]`; do not overwrite existing tests unless a `quality_fixes` instruction explicitly targets that scenario.
- Preserve existing `expect` blocks unless revising that scenario is required.
- Add new branches to `branch_manifest[]`; extend `scenarios[]` for existing branches.
- Recalculate `uncovered_branches`, `coverage`, and `status` from the merged result.

## Ownership rule

- `generating-tests` sets `coverage` and `status` on the generated spec.
- `reviewing-tests` independently audits coverage and may approve, warn, or kick back the result.
