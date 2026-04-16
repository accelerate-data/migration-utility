# Review Checks

Use these checklists after gathering context. They supplement, not replace, the
shared standards references.

## Correctness

Compare the generated model to `proc_body`.

Verify:

- all `source_tables` are represented
- all target `columns` reach the final CTE
- joins and filters preserve source semantics
- aggregation grain matches
- `UPDATE ... FROM` rewrites preserve target-row retention semantics
- source-driven full-refresh rewrites do not drop rows that the original routine
  would have left unchanged
- if the original routine updates an existing target table, the reviewed model
  reads that target relation unless the procedure is clearly full-refresh
  replacement logic
- incremental logic matches MERGE intent where applicable

Use `REVIEW_CORRECTNESS_GAP` for correctness failures.

## Test Integration

Read `test-specs/<item_id>.json` and compare it to the generated schema YAML.

Verify:

- the approved test spec exists and is readable
- `unit_tests:` are rendered when the spec contains `unit_tests[]`
- scenario names match the approved spec
- unit tests target the reviewed `model_name`
- required schema tests remain present in YAML

Use `REVIEW_TEST_INTEGRATION_GAP` for test-integration failures.

## Standards

Read and apply all rules in:

- [dbt-project-standards.md](../../_shared/references/dbt-project-standards.md)
- [sql-style.md](../../_shared/references/sql-style.md)
- [cte-structure.md](../../_shared/references/cte-structure.md)
- [model-naming.md](../../_shared/references/model-naming.md)
- [yaml-style.md](../../_shared/references/yaml-style.md)
- [model-artifact-invariants.md](../../_shared/references/model-artifact-invariants.md)

Pay special attention to these recurring failures:

- SQL style and casing
- CTE order and `final` shape
- model/file naming and layer rules
- model materialization matches the derived profile
- ordinary first-pass mart tables rely on the `marts` layer default
- view and materialized-view profiles (`classification: stg|mart`) use explicit
  `materialized='view'`, not `ephemeral`
- `MDL_016`: mart SQL uses `ref('stg_bronze__<entity>')` instead of direct
  `source('bronze', '<table>')` for confirmed source dependencies
- if an expected staging wrapper is missing, report the missing setup artifact
  instead of approving direct source use
- `MDL_017`: mart SQL uses `ref('<seed_name>')` for catalog tables marked
  `is_seed: true` when they appear in joins or filters
- seed dependencies referenced through direct `source()` or raw warehouse names
  are standards violations
- seed dependency classification takes precedence over generic confirmed-source
  classification; report `MDL_017`, not `MDL_016`, when catalog metadata marks
  the relation `is_seed: true`
- `dbt_project.yml` layer materialization defaults
- no redundant `materialized='table'` config on ordinary mart table models
- YAML indentation and structure beyond the shared invariants

This callout list highlights common misses but does not limit the referenced
standards. Use `REVIEW_STANDARDS_VIOLATION` in `checks.standards.issues[]` and
stable standards codes (`SQL_*`, `CTE_*`, `MDL_*`, `YML_*`) in
`feedback_for_model_generator[]`.
