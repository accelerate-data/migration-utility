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

| Situation | Action |
|---|---|
| Readiness not met | Run `migrate-util ready`; surface the failing `code` and `reason`; stop |
| Multi-table writer | Use `writer_ddl_slice` as the primary SQL input |
| View target | Read view `refactored_sql`; use profile classification to choose materialization |
| Reviewer handoff present | Use `artifact_paths` and `revision_feedback` exactly as provided |
| No reviewer handoff | Derive paths from `migrate write` output; assume no revision feedback |
| Connection failure on `dbt compile` | Fall back to `dbt parse`; skip `dbt test` |

Before returning `ok` or `partial`, make sure the written artifacts satisfy [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md).

## Happy Path

1. Check readiness.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> generate
   ```

   If `ready` is `false`, report the failing `code` and `reason` and stop.

2. Assemble deterministic context.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
     --table <table_fqn>
   ```

   Use `refactored_sql` as the primary SQL input. If `writer_ddl_slice` is present, use it instead for this table. Do not generate from `proc_body`.

3. Generate SQL that preserves the transformed logic.

   Apply:
   - [../_shared/references/sql-style.md](../_shared/references/sql-style.md)
   - [../_shared/references/cte-structure.md](../_shared/references/cte-structure.md)
   - [../_shared/references/model-naming.md](../_shared/references/model-naming.md)
   - [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md)

   Rules:
   - Keep one dbt model artifact per target. Do not split one target across multiple helper SQL files.
   - Use `{{ source('<schema>', '<table>') }}` directly in import CTEs.
   - Preserve joins, filters, grouping, and write intent from `refactored_sql`.
   - Add required control columns in the final projection: `_dbt_run_id = {{ invocation_id }}` for all materializations, plus `_loaded_at = current_timestamp()` for table and snapshot materializations.
   - For snapshots, use the snapshot-specific guidance in [references/snapshot-generation.md](references/snapshot-generation.md). Snapshot config changes file shape, not business logic.

4. Run a logical equivalence pass against `refactored_sql`.

   Check source tables, selected columns, join conditions, filter predicates, aggregation grain, and write semantics.

   - If the difference is stylistic or dialect-normalizing, proceed and note it only if useful.
   - If a semantic gap remains, record `EQUIVALENCE_GAP` in `warnings[]` and continue.

5. Build schema YAML.

   Apply [../_shared/references/yaml-style.md](../_shared/references/yaml-style.md).

   Include deterministic schema tests from context:
   - PK -> `unique` and `not_null`
   - FK -> `relationships`
   - PII -> `meta`
   - watermark -> `recency` for incremental models

6. Render canonical unit tests from the approved test spec.

   First write the draft schema YAML to a temp file, then render into that file:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate render-unit-tests \
     --table <table_fqn> \
     --model-name <model_name> \
     --spec test-specs/<item_id>.json \
     --schema-yml .staging/schema.yml \
     --project-root <project_root>
   ```

   The CLI is the source of truth for canonical `unit_tests:` rendering. Do not hand-write those scenarios.

7. Write artifacts through the CLI.

   If the caller supplied a handoff object:
   - use `artifact_paths` exactly as given
   - use `revision_feedback` exactly as given
   - do not read or interpret `.migration-runs/` sweep artifacts

   Write SQL and YAML to temp files, then call:

   ```bash
   mkdir -p .staging
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write \
     --table <table_fqn> \
     --model-sql-file .staging/model.sql \
     --schema-yml-file .staging/schema.yml \
     --project-root <project_root>
   ```

   Use the CLI-returned written paths when populating `artifact_paths`. Do not hardcode output paths when the CLI is the writer of record.

8. Validate with dbt.

   ```bash
   cd "${DBT_PROJECT_PATH:-./dbt}" && dbt compile --select <model_name>
   cd "${DBT_PROJECT_PATH:-./dbt}" && dbt test --select <model_name>
   ```

   If compile fails because the warehouse is unavailable, say so, run `dbt parse`, report the offline result, and skip `dbt test`.

   If compile or test fails for model reasons:
   - revise the model SQL only
   - re-run `migrate write`
   - re-run `dbt compile` and `dbt test`
   - stop after 3 total self-correction attempts

9. Add gap tests only after canonical tests pass.

   Add 1-3 `test_gap_*` scenarios for uncovered logic branches, re-write the schema YAML, and run `dbt test` once more. Gap tests are mutable; canonical test-spec scenarios are not.

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

The generator owns generation facts, not reviewer judgment. If the output contract still requires a `review` object before reviewer execution, keep it minimal and factual rather than inventing a review narrative.

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
