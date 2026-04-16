---
name: generating-model
description: Use when generating or revising one dbt model for a single profiled table or view after refactor and approved test-spec work are complete.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Model

Generate or revise one dbt artifact set from deterministic migration context.

**Core principle:** preserve the semantics of `refactored_sql`; style and materialization choices must not change business logic.

Use the canonical codes in [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md). Return one JSON object matching `ModelGenerationOutput` in [../../lib/shared/output_models/model_generation.py](../../lib/shared/output_models/model_generation.py).

## When to Use

- One table or view is ready for model generation.
- `/generate-model` is delegating a single item.
- `/reviewing-model` requested a revision and supplied structured feedback.

Do not use this skill for batch orchestration. `/generate-model` owns batching, review loops, commits, and summaries.

## Quick Reference

- Readiness failure: surface the failing `code` and `reason`, then stop.
- Multi-table writer: use `writer_ddl_slice`; otherwise use `refactored_sql`.
- Reviewer handoff: use `artifact_paths` and `revision_feedback` exactly as given.
- Offline compile: fall back to `dbt parse` and skip dbt execution.
- Before returning `ok` or `partial`, satisfy [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md).

## Happy Path

1. Check readiness.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util ready generate --object <table_fqn>
   ```

2. Assemble deterministic context.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate context \
     --table <table_fqn>
   ```

   Use `writer_ddl_slice` when present; otherwise use `refactored_sql`. Never generate from `proc_body`.

3. Generate mart SQL that preserves the transformed logic.

   Apply [dbt-project-standards](../_shared/references/dbt-project-standards.md), [sql-style](../_shared/references/sql-style.md), [cte-structure](../_shared/references/cte-structure.md), [model-naming](../_shared/references/model-naming.md), and [model-artifact-invariants](../_shared/references/model-artifact-invariants.md).

   Rules:
   - Every generated model SQL artifact must start with a dbt `config(` block inside `{{ ... }}` before any CTEs or `select`. Snapshot SQL starts with `{% snapshot <name> %}`, then immediately includes the `config(` block before any CTEs or `select`.
   - Keep one dbt mart model artifact per target. Do not split one target across multiple helper SQL files.
   - Do not write migrated target SQL or YAML under `models/staging/`. That folder is only for source YAML and pure `stg_bronze__*` wrappers from `setup-target`. If you are about to write `models/staging/<target>.sql`, stop and use the CLI writer instead.
   - First-pass generated table/view targets write to `models/marts/<model_name>.sql` through `migrate write`.
   - Before drafting SQL, inspect `models/staging/_staging__models.yml` and `models/staging/stg_bronze__*.sql`. Use `{{ ref('stg_bronze__<entity>') }}` for confirmed bronze source relations when the pass-through staging wrapper exists.
   - Translate raw bronze references in `refactored_sql` to staging wrapper refs during generation. Do not leave `{{ source('bronze', '<table>') }}` in a mart when a matching `stg_bronze__<entity>` wrapper exists.
   - Use `{{ source('bronze', '<table>') }}` directly only when no matching staging wrapper exists.
   - Preserve joins, filters, grouping, and write intent from `refactored_sql`.
   - Add required control columns from `model-artifact-invariants` with the exact standard expressions: `'{{ invocation_id }}' as _dbt_run_id` for every generated artifact and `{{ current_timestamp() }} as _loaded_at` for table and snapshot materializations.
   - When the target comes from `catalog/views/` (view or materialized view profile), generate the dbt model with `materialized='view'`. Do not use `ephemeral` for generated view models.
   - For snapshots, use [references/snapshot-generation.md](references/snapshot-generation.md).

4. Run a logical equivalence pass against `refactored_sql`.

   Check source tables, selected columns, joins, filters, grain, and write semantics. Record `EQUIVALENCE_GAP` in `warnings[]` if a semantic gap remains.

5. Build schema YAML.

   Apply [yaml-style](../_shared/references/yaml-style.md). Add deterministic tests from context: PK -> `unique` and `not_null`, FK -> `relationships`, PII -> `meta`, watermark -> `recency`.

   The generated YAML must describe the mart model. `migrate write` merges it into `models/marts/_marts__models.yml`; do not create one YAML file per generated model.

6. Render canonical unit tests from the approved test spec.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate render-unit-tests \
     --table <table_fqn> \
     --model-name <model_name> \
     --spec test-specs/<item_id>.json \
     --schema-yml .staging/schema.yml \
     --project-root <project_root>
   ```

   The CLI is the source of truth for canonical `unit_tests:`. Do not hand-write them.

7. Write artifacts through the CLI.

   If the caller supplied a handoff object:
   - use `artifact_paths` exactly as given
   - use `revision_feedback` exactly as given

   Then write SQL and YAML through:

   ```bash
   mkdir -p .staging
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate write \
     --table <table_fqn> \
     --model-sql-file .staging/model.sql \
     --schema-yml-file .staging/schema.yml \
     --project-root <project_root>
   ```

   Use the CLI-returned written paths. Do not hardcode output paths.
   Do not use direct file writes for table or view target SQL/YAML. Direct writes are only allowed for snapshot artifacts when the CLI explicitly cannot write the snapshot path.

8. Validate with dbt using the manifest runtime roles.

   Read `manifest.json` at the project root and use the canonical runtime contract:
   - `runtime.target` is the dbt validation target
   - `runtime.sandbox` is the source-relation execution endpoint when the workflow requires sandbox-backed validation

   Do not read flat fields such as `sandbox.database`. Do not derive `target` from `source` or `sandbox`. Use the fully specified runtime roles from the manifest and map them to the backend-specific environment required by the dbt profile in the eval or project environment.

   ```bash
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt compile --select <model_name>
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt build --select <model_name>
   ```

   Use `dbt build` rather than `dbt test` alone so the generated model relation is materialized before generic tests run. If the warehouse is unavailable, run `dbt parse` and skip dbt execution. If build fails because of target environment state rather than model SQL, such as an existing relation case conflict or adapter/runtime DDL limitation, run `dbt parse`, record the skipped build in `warnings[]`, and stop instead of rewriting business SQL. If compile or build fails for model reasons, revise SQL, re-write, and retry up to 3 total attempts.

9. Add gap tests only after canonical tests pass.

   Add 1-3 `test_gap_*` scenarios only for uncovered logic branches. Canonical test-spec scenarios are not mutable.

10. Write generation status to catalog.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate write-catalog \
     --table <table_fqn> \
     --model-path <relative_model_sql_path> \
     --compiled <true|false> \
     --tests-passed <true|false> \
     --test-count <number> \
     --schema-yml <true|false> \
     --project-root <project_root>
   ```

   Pass `--warnings` and `--errors` as JSON arrays when needed.

## Review Handoff

If `/reviewing-model` sent `revision_feedback`, treat it as bounded revision input:

- revise the existing model rather than regenerating from scratch unless the feedback requires a full rewrite
- preserve canonical unit tests
- re-run validation after changes

The generator owns generation facts, not reviewer judgment.

## Common Mistakes

- Using `proc_body` as the generation source. Use `refactored_sql`, or `writer_ddl_slice` for multi-table writers.
- Creating staging models for migrated target logic. Initial staging wrappers are generated by `setup-target`; transformed migrated targets are marts unless they are snapshots.
- Hardcoding `migrate write` output paths. The CLI decides written paths; report what it returned.
- Reducing snapshot models to raw `select * from {{ source(...) }}`. Snapshot config may change, but transformed logic must still be preserved.
- Hand-writing canonical `unit_tests:` blocks. Use `migrate render-unit-tests`.
- Returning `ok` for artifacts that do not satisfy the shared artifact invariants.
- Treating review state as generation state. The reviewer owns review findings; generator revisions respond to them.

## References

- [../../lib/shared/output_models/model_generation.py](../../lib/shared/output_models/model_generation.py) — structured input/output contract
- [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md) — canonical statuses and surfaced codes
- [references/snapshot-generation.md](references/snapshot-generation.md) — snapshot-specific generation rules
