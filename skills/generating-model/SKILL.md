---
name: generating-model
description: Use when generating or revising one dbt model for a single profiled table or view after refactor and approved test-spec work are complete.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Model

Generate or revise one dbt artifact set from deterministic migration context.

**Core principle:** preserve the selected transformed SQL. Use `writer_ddl_slice` for multi-table writers; otherwise use `refactored_sql`. Style and materialization choices must not change business logic.

Use the canonical codes in [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md). Return one JSON object matching `ModelGenerationOutput` in [../../lib/shared/output_models/model_generation.py](../../lib/shared/output_models/model_generation.py).

## When to Use

- One table or view is ready for model generation.
- `/generate-model` is delegating a single item.
- `/reviewing-model` requested a revision and supplied structured feedback.

Do not use this skill for batch orchestration. `/generate-model` owns batching, review loops, commits, and summaries.

## Quick Reference

- Readiness failure: surface the failing `code` and `reason`, then stop. If readiness has no canonical code, use the closest shared code; otherwise use `GENERATION_FAILED`.
- Multi-table writer: use `writer_ddl_slice`; otherwise use `refactored_sql`.
- Reviewer handoff: use `artifact_paths` and `revision_feedback` exactly as given.
- Offline compile: fall back to `dbt parse` and skip dbt execution.
- Before returning `ok` or `partial`, satisfy [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md).
- Missing confirmed staging wrapper: return `status: "error"` with `GENERATION_FAILED`.
- Do not create or mutate test-spec scenarios. Report uncovered logic as warnings for `/generate-tests`.

Return exactly one `ModelGenerationOutput`. Use `execution.dbt_test_passed` for the `dbt build` result. For snapshots, `artifact_paths` must use the CLI-returned `snapshots/...` paths.

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

3. Generate target dbt SQL that preserves the transformed logic.

   Apply [dbt-project-standards](../_shared/references/dbt-project-standards.md), [sql-style](../_shared/references/sql-style.md), [cte-structure](../_shared/references/cte-structure.md), [model-naming](../_shared/references/model-naming.md), and [model-artifact-invariants](../_shared/references/model-artifact-invariants.md).

   Generation decisions:
   - Generate from `refactored_sql` or `writer_ddl_slice`; preserve joins, filters, grouping, and write intent.
   - Use project defaults for ordinary mart tables. Add model-level `config(` only for exceptions: aliases, schemas, incremental models, snapshots, or view materialization.
   - Use `{{ ref('stg_bronze__<entity>') }}` for confirmed source dependencies and `{{ ref('<seed_name>') }}` for seed dependencies. If a confirmed source wrapper is missing, stop with `GENERATION_FAILED`.
   - Produce one target artifact: first-pass tables/views are marts; snapshots follow [references/snapshot-generation.md](references/snapshot-generation.md).
   - Include required control columns from `model-artifact-invariants` with the exact standard expressions.

4. Run a logical equivalence pass against the selected transformed SQL.

   Check source tables, selected columns, joins, filters, grain, and write semantics. Record `EQUIVALENCE_GAP` in `warnings[]` if a semantic gap remains.

5. Build schema YAML.

   Apply [yaml-style](../_shared/references/yaml-style.md). Add deterministic tests from context: PK -> `unique` and `not_null`, FK -> `relationships`, PII -> `meta`, watermark -> `recency`.

   The generated YAML must describe the target artifact. `migrate write` merges mart YAML into `models/marts/_marts__models.yml` and snapshot YAML into `snapshots/_snapshots__models.yml`.

6. Render canonical unit tests from the approved test spec.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate render-unit-tests \
     --table <table_fqn> \
     --model-name <model_name> \
     --spec test-specs/<item_id>.json \
     --schema-yml .staging/schema.yml \
     --project-root <project_root>
   ```

   The CLI is the source of truth for canonical `unit_tests:`. It maps confirmed source fixtures to `ref('stg_bronze__<entity>')`. Do not hand-write them.

7. Write artifacts through the CLI.

   If the caller supplied a handoff object, use `artifact_paths` and `revision_feedback` exactly as given.

   Then write SQL and YAML through:

   ```bash
   mkdir -p .staging
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate write \
     --table <table_fqn> \
     --model-sql-file .staging/model.sql \
     --schema-yml-file .staging/schema.yml \
     --project-root <project_root>
   ```

   Use the CLI-returned written paths. Do not hardcode output paths or use direct file writes for SQL/YAML; the CLI writes mart and snapshot artifacts.

8. Validate with dbt using the manifest runtime roles.

   Read `manifest.json` at the project root and use the canonical runtime contract:
   - `runtime.target` is the dbt validation target
   - `runtime.sandbox` is the source-relation execution endpoint when the workflow requires sandbox-backed validation

   Do not read flat fields such as `sandbox.database`. Do not derive `target` from `source` or `sandbox`.

   ```bash
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt compile --select <model_name>
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt build --select <model_name>
   ```

   Use `dbt build`, not `dbt test` alone. Record it in `execution.dbt_test_passed`. If the warehouse is unavailable or the target environment fails independently of model SQL, run `dbt parse`, warn, and do not rewrite business SQL. If compile/build fails for model reasons, revise, re-write, and retry up to 3 total attempts.

9. Record test gaps without mutating approved specs.

   If canonical tests expose missing branch coverage, add a warning that names the uncovered branch and leave new scenario creation to `/generate-tests`.

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
   Catalog status is `ok` only when the written artifact exists and compile/build passed. Written artifacts with compile/build warnings persist as `partial`. Missing or unusable artifacts persist as `error`.

## Review Handoff

If `/reviewing-model` sent `revision_feedback`, treat it as bounded revision input:

- revise the existing model rather than regenerating from scratch unless the feedback requires a full rewrite
- preserve canonical unit tests
- re-run validation after changes

The generator owns generation facts, not reviewer judgment.

## Common Mistakes

- Using `proc_body` as the generation source. Use `refactored_sql`, or `writer_ddl_slice` for multi-table writers.
- Creating staging models for migrated target logic. Initial staging wrappers are generated by `setup-target`; transformed migrated targets are marts unless they are snapshots.
- Using `source('bronze', ...)` in generated marts when a confirmed `stg_bronze__*` wrapper exists.
- Hardcoding `migrate write` output paths. The CLI decides written paths; report what it returned.
- Reducing snapshot models to raw `select * from {{ source(...) }}`. Snapshot config may change, but transformed logic must still be preserved.
- Hand-writing canonical `unit_tests:` blocks. Use `migrate render-unit-tests`.
- Adding `test_gap_*` scenarios during model generation. Report gaps; `/generate-tests` owns spec changes.
- Returning `ok` for artifacts that do not satisfy the shared artifact invariants.
- Treating review state as generation state. The reviewer owns review findings; generator revisions respond to them.

## References

- [../../lib/shared/output_models/model_generation.py](../../lib/shared/output_models/model_generation.py) — structured input/output contract
- [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md) — canonical statuses and surfaced codes
- [references/snapshot-generation.md](references/snapshot-generation.md) — snapshot-specific generation rules
