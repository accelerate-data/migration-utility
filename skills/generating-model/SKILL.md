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
- Offline compile: fall back to `dbt parse` and warn.
- Before returning `ok` or `partial`, satisfy [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md).
- Missing confirmed staging wrapper: return `status: "error"` with `GENERATION_FAILED`.
- Do not create or mutate test-spec scenarios. Report uncovered logic as warnings for `/generate-tests`.

Return exactly one `ModelGenerationOutput`. Use `execution.dbt_test_passed` for the `dbt build` result. For snapshots, `artifact_paths` must use the CLI-returned `snapshots/...` paths.

## Happy Path

1. Check readiness.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util ready generate --object <table_fqn>
   ```

2. Assemble deterministic context and choose the generation source.

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate context \
     --table <table_fqn>
   ```

   Follow [context-selection.md](references/context-selection.md). Never generate from `proc_body`.

3. Generate target dbt SQL that preserves the transformed logic.

   Apply [dbt-project-standards](../_shared/references/dbt-project-standards.md), [sql-style](../_shared/references/sql-style.md), [cte-structure](../_shared/references/cte-structure.md), [model-naming](../_shared/references/model-naming.md), and [model-artifact-invariants](../_shared/references/model-artifact-invariants.md).

   Use project defaults for ordinary mart tables. Add model-level `config(` only for exceptions: aliases, schemas, incremental models, snapshots, or view materialization. For source/seed refs and missing wrapper handling, follow [artifact-writing.md](references/artifact-writing.md). For snapshots, follow [snapshot-generation.md](references/snapshot-generation.md).

4. Run a logical equivalence pass against the selected transformed SQL.

   Follow [context-selection.md](references/context-selection.md). Record `EQUIVALENCE_GAP` in `warnings[]` if a semantic gap remains.

5. Build schema YAML.

   Apply [yaml-style](../_shared/references/yaml-style.md) and [artifact-writing.md](references/artifact-writing.md). Add deterministic tests from context: PK -> `unique` and `not_null`, FK -> `relationships`, PII -> `meta`, watermark -> `recency`.

6. Render canonical unit tests from the approved test spec.

   Follow [artifact-writing.md](references/artifact-writing.md). The CLI is the source of truth for canonical `unit_tests:`. Do not hand-write them.

7. Write artifacts through the CLI.

   If the caller supplied a handoff object, use `artifact_paths` and `revision_feedback` exactly as given.

   Follow [artifact-writing.md](references/artifact-writing.md). Use the CLI-returned written paths. Do not hardcode output paths or use direct file writes.

8. Validate with dbt using the manifest runtime roles.

   Follow [validation.md](references/validation.md). Use `dbt build`, not `dbt test` alone. Record it in `execution.dbt_test_passed`.

9. Record test gaps without mutating approved specs.

   If canonical tests expose missing branch coverage, add a warning that names the uncovered branch and leave new scenario creation to `/generate-tests`.

10. Write generation status to catalog.

   Follow [artifact-writing.md](references/artifact-writing.md). Pass `--warnings` and `--errors` as JSON arrays when needed.

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
- [references/context-selection.md](references/context-selection.md) — source SQL selection and equivalence checks
- [references/artifact-writing.md](references/artifact-writing.md) — unit tests, CLI writes, and catalog status
- [references/validation.md](references/validation.md) — dbt compile/build/parse handling
- [references/snapshot-generation.md](references/snapshot-generation.md) — snapshot-specific generation rules
