# Model Artifact Invariants

These are the shared artifact standards for one generated dbt target.

- SQL artifact exists and is the single reviewable model for the target.
- Ordinary first-pass mart table models rely on `dbt_project.yml` layer defaults and must not set redundant `materialized='table'` config.
- SQL uses a dbt `config(` block only for exceptions such as aliases, schemas, incremental models, snapshots, or view materialization.
- Required control columns follow [model-naming.md](model-naming.md):
  - `_dbt_run_id = '{{ invocation_id }}'` for all materializations
  - `_loaded_at = {{ current_timestamp() }}` for table mart and snapshot materializations
- Paired schema YAML exists (`models/.../_*_models.yml` or `snapshots/_snapshots__models.yml`).
- Schema YAML contains `version: 2`.
- Schema YAML contains the required model or snapshot description.
- Canonical `unit_tests:` from the approved spec are rendered in YAML when the spec includes them.

`/generating-model` must produce artifacts that satisfy these invariants before returning `ok` or `partial`.

`/reviewing-model` must verify these invariants directly from disk instead of trusting generator self-report.
