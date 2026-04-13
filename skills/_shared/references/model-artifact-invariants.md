# Model Artifact Invariants

These are the shared artifact standards for one generated dbt target.

- SQL artifact exists and is the single reviewable model for the target.
- SQL contains a dbt `config(` block.
- Required control columns follow [model-naming.md](model-naming.md):
  - `_dbt_run_id = '{{ invocation_id }}'` for all materializations
  - `_loaded_at = {{ current_timestamp() }}` for table and snapshot materializations
- Paired schema YAML exists.
- Schema YAML contains `version: 2`.
- Schema YAML contains the required model description.
- Canonical `unit_tests:` from the approved spec are rendered in YAML when the spec includes them.

`/generating-model` must produce artifacts that satisfy these invariants before returning `ok` or `partial`.

`/reviewing-model` must verify these invariants directly from disk instead of trusting generator self-report.
