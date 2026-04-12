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
- Offline compile: fall back to `dbt parse` and skip `dbt test`.
- Before returning `ok` or `partial`, satisfy [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md).

## Happy Path

1. Check readiness.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> generate
   ```

2. Assemble deterministic context.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
     --table <table_fqn>
   ```

   Use `writer_ddl_slice` when present; otherwise use `refactored_sql`. Never generate from `proc_body`.

3. Generate SQL that preserves the transformed logic.

   Apply [sql-style](../_shared/references/sql-style.md), [cte-structure](../_shared/references/cte-structure.md), [model-naming](../_shared/references/model-naming.md), and [model-artifact-invariants](../_shared/references/model-artifact-invariants.md).

   Rules:
   - Keep one dbt model artifact per target. Do not split one target across multiple helper SQL files.
   - Use `{{ source('<schema>', '<table>') }}` directly in import CTEs for raw source relations.
   - Preserve joins, filters, grouping, and write intent from `refactored_sql`.
   - When the target comes from `catalog/views/` (view or materialized view profile), generate the dbt model with `materialized='view'`. Do not use `ephemeral` for generated view models.
   - For snapshots, use [references/snapshot-generation.md](references/snapshot-generation.md).

4. Run a logical equivalence pass against `refactored_sql`.

   Check source tables, selected columns, joins, filters, grain, and write semantics. Record `EQUIVALENCE_GAP` in `warnings[]` if a semantic gap remains.

5. Build schema YAML.

   Apply [yaml-style](../_shared/references/yaml-style.md). Add deterministic tests from context: PK -> `unique` and `not_null`, FK -> `relationships`, PII -> `meta`, watermark -> `recency`.

6. Render canonical unit tests from the approved test spec.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate render-unit-tests \
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
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write \
     --table <table_fqn> \
     --model-sql-file .staging/model.sql \
     --schema-yml-file .staging/schema.yml \
     --project-root <project_root>
   ```

   Use the CLI-returned written paths. Do not hardcode output paths.

8. Validate with dbt against the sandbox.

   Read `manifest.json` at the project root and extract `sandbox.database` and `technology`. Use the sandbox value to override the appropriate env var so that `dbt compile` and `dbt test` resolve source relations against the sandbox, which contains cloned schema from the source system.

   | Technology | Sandbox type | Env var override |
   |---|---|---|
   | `sql_server`, `fabric_warehouse` | database | `MSSQL_DB=<sandbox_database>` |
   | `oracle` | schema | `DBT_SCHEMA=<sandbox_database>` |

   ```bash
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt compile --select <model_name>
   cd "${DBT_PROJECT_PATH:-./dbt}" && <ENV_OVERRIDE> dbt test --select <model_name>
   ```

   If the warehouse is unavailable, run `dbt parse` and skip `dbt test`. If compile or test fails for model reasons, revise SQL, re-write, and retry up to 3 total attempts.

9. Add gap tests only after canonical tests pass.

   Add 1-3 `test_gap_*` scenarios only for uncovered logic branches. Canonical test-spec scenarios are not mutable.

10. Write generation status to catalog.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write-catalog \
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
- Hardcoding `migrate write` output paths. The CLI decides written paths; report what it returned.
- Reducing snapshot models to raw `select * from {{ source(...) }}`. Snapshot config may change, but transformed logic must still be preserved.
- Hand-writing canonical `unit_tests:` blocks. Use `migrate render-unit-tests`.
- Returning `ok` for artifacts that do not satisfy the shared artifact invariants.
- Treating review state as generation state. The reviewer owns review findings; generator revisions respond to them.

## References

- [../../lib/shared/output_models/model_generation.py](../../lib/shared/output_models/model_generation.py) — structured input/output contract
- [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md) — canonical statuses and surfaced codes
- [references/snapshot-generation.md](references/snapshot-generation.md) — snapshot-specific generation rules
