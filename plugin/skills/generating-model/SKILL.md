---
name: generating-model
description: >
  Generates a dbt model from a source routine. Requires catalog profile,
  resolved statements from prior discover + profile stages, and an approved
  test spec from the test-generation stage.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Generating Model

Generate one dbt model for one table or view. Use deterministic context from catalog, generate dbt SQL and YAML, validate equivalence, write artifacts, run dbt validation, and return one JSON result.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing.

## Before invoking

Use the canonical `/generate-model` codes in [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md).

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> generate
```

If `ready` is `false`, report the failing check's `code` and `reason` to the user and stop.

## Caller handoff

The caller may provide a structured handoff object:

```json
{
  "artifact_paths": {"model_sql": "...", "model_yaml": "..."} | null,
  "revision_feedback": [{"code": "SQL_001", "message": "...", "severity": "error | warning | info", "ack_required": true}] | null
}
```

If a handoff is provided:

- use `artifact_paths` exactly as given
- use `revision_feedback` exactly as given
- do not read or interpret sweep artifacts from `.migration-runs/`

If no handoff is provided:

- derive `artifact_paths` locally
- assume no `revision_feedback`

## Output contract

Return exactly one JSON object:

```json
{
  "item_id": "<table_fqn>",
  "status": "ok | partial | error",
  "output": {
    "table_ref": "<table_fqn>",
    "model_name": "<model_name>",
    "artifact_paths": {"model_sql": "...", "model_yaml": "..."},
    "generated": {
      "model_sql": {"materialized": "<materialization>", "uses_watermark": bool},
      "model_yaml": {"has_model_description": bool, "schema_tests_rendered": [...], "has_unit_tests": bool}
    },
    "execution": {"dbt_compile_passed": bool, "dbt_test_passed": bool, "self_correction_iterations": int, "dbt_errors": []},
    "review": {"iterations": int, "verdict": "approved | approved_with_warnings"},
    "warnings": [],
    "errors": []
  }
}
```

Return only this JSON object. Use codes from [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md) in `warnings[]` and `errors[]`.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <table_fqn>
```

Use `refactored_sql` as your sole SQL input. Ignore `proc_body` and `statements` — they are not relevant to model generation.

**Multi-table-writer:** If `writer_ddl_slice` is present in the context, the writer is a multi-table proc. Use `writer_ddl_slice` as the primary SQL for this model — it contains only the logic for this table. The full `proc_body` is for reference only.

**View detection:** If the catalog object is a view (`catalog/views/<fqn>.json` exists), the refactored SQL lives in `catalog/views/<fqn>.json → refactor.refactored_sql` instead of the procedure catalog. The materialization is determined by the view's profile `classification`:

- `stg` classification → `materialized='ephemeral'` staging model
- `mart` classification → use the same materialization decision rules as tables (table/incremental/snapshot based on profile signals)

## Step 2: Generate dbt SQL

Produce the model SQL from the `refactored_sql`. Apply [sql-style.md](../_shared/references/sql-style.md) (keywords, indentation, commas) and [cte-structure.md](../_shared/references/cte-structure.md) (import/logical/final pattern) throughout. Apply [model-naming.md](../_shared/references/model-naming.md) for layer prefixes, `_dbt_run_id`, and `_loaded_at` rules.

**Source table references:** All source table references use `{{ source('<schema>', '<table>') }}` directly in import CTEs. Do not generate separate `stg_*.sql` files.

Follow the import → logical → final CTE pattern from cte-structure.md. For incremental models, add `unique_key` and `incremental_strategy='merge'` to the config, and add the watermark filter in the appropriate logical CTE. For snapshot models, follow [references/snapshot-generation.md](references/snapshot-generation.md).

## Step 3: Logical equivalence check

Compare the generated model against `refactored_sql`. Check each of these:

| Check | What to compare |
|---|---|
| Source tables | Same tables read in generated model vs original proc? |
| Columns selected | Same columns in final SELECT vs original INSERT column list? |
| Join conditions | Same join keys and join types (INNER/LEFT/RIGHT/FULL)? |
| Filter predicates | Same WHERE/HAVING conditions (modulo syntax differences)? |
| Aggregation grain | Same GROUP BY columns? |
| Write semantics | INSERT/MERGE/UPDATE intent preserved by materialization? |

For each check:

- **Match**: proceed silently
- **Intentional divergence** (e.g., dialect-specific functions replaced with ANSI equivalents (e.g. `ISNULL` → `COALESCE`, `NVL` → `COALESCE`)): note as informational
- **Semantic gap** (missing join, different grain, dropped column): record an `EQUIVALENCE_GAP` warning

If warnings exist, record them in the item result and continue.

## Step 4: Build schema.yml

Apply [yaml-style.md](../_shared/references/yaml-style.md) (indentation, `version: 2`, required descriptions) throughout.

### 4a — Schema tests

Render `schema_tests` from context into the `columns:` section following yaml-style.md. Include `unique` and `not_null` for PK columns, `relationships` for FK columns, PII `meta` tags, and `recency` for incremental models with watermark.

### 4b — Render test-spec unit tests

Run the CLI to render unit tests from the test spec into schema.yml:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate render-unit-tests \
  --table <fqn> --model-name <model_name> \
  --spec test-specs/<item_id>.json \
  --schema-yml <schema_yml_path> \
  --project-root <project_root>
```

This writes the canonical test scenarios from the test spec into the `unit_tests:` block of the schema YAML. The CLI is the single source of truth for unit test rendering — do not manually construct the YAML block.

## Step 5: Prepare final result

Include these in the final item result:

1. generated model SQL
2. schema YAML
3. equivalence warnings, if any
4. materialization and config decisions

## Step 6: Write artifacts

Before writing, decide the exact output paths.

If the caller supplied `artifact_paths`, use them exactly. Otherwise decide them locally:

- model → `dbt/models/<layer>/<model_name>.sql` and `dbt/models/<layer>/_<model_name>.yml`
- snapshot → `dbt/snapshots/<model_name>.sql` and `dbt/snapshots/schema.yml`

Write the generated SQL and YAML to temporary files first to avoid shell escaping issues with multi-line content:

1. Write the model SQL to `.staging/model.sql`
2. Write the schema YAML to `.staging/schema.yml`

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write \
  --table <table_fqn> \
  --model-path <relative_path_to_model.sql> \
  --schema-yml-path <relative_path_to_schema.yml> \
  --model-sql-file .staging/model.sql \
  --schema-yml-file .staging/schema.yml && rm -rf .staging
```

Use the CLI-returned written paths when constructing the final item result.

## Step 7: Compile and run canonical tests

### 7a — Compile

Run `dbt compile` to verify the generated model compiles:

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt compile --select <model_name>
```

If compile fails with a **connection error** (adapter cannot reach the warehouse — look for "Could not connect", "Login failed", "Connection refused", or similar adapter errors):

1. Tell the user: "No warehouse connection available. Falling back to offline validation."
2. Run `dbt parse` in the dbt project directory instead.
3. Report parse results. If parse fails, attempt to fix (max 3 iterations as below). Skip `dbt test` — unit tests require compilation.

If compile fails with a **non-connection error** (syntax, bad ref, macro resolution), proceed to the self-correction loop in 7c.

### 7b — Run canonical unit tests

On compile success, run unit tests:

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt test --select <model_name>
```

All canonical tests (from the test spec) must pass before proceeding.

### 7c — Self-correction loop (max 3 iterations)

If compile or test fails:

1. Analyze the failure output — identify which test failed and why (wrong column, missing row, type mismatch, etc.).
2. Revise the model SQL to fix the issue. Do not modify test-spec unit tests — they are immutable ground truth. Only the model SQL is mutable during self-correction.
3. Re-run `migrate write` with the revised SQL and schema YAML.
4. Re-run `dbt compile` and `dbt test`.
5. Repeat up to 3 iterations total.

After 3 failed iterations:

- Report the failing test names and error details to the user.
- Leave the model as-is with `status: "partial"`.
- Record failures in `execution.dbt_errors[]`.

## Step 8: Gap tests

Run this step only after all canonical tests pass in Step 7.

Analyze the generated model's logic for branches not covered by existing test-spec scenarios. Look for:

- JOIN conditions with no matching/non-matching test case
- CASE/WHEN arms not exercised
- NULL handling paths (dialect-specific function replacements)
- Incremental filter (`is_incremental()`) boundary cases
- Empty source table edge cases

Generate 1-3 additional unit test scenarios for uncovered branches. Add them to the `unit_tests:` block in the schema YAML alongside the canonical scenarios. Use the naming convention `test_gap_<description>` to distinguish LLM-generated tests from ground-truth test-spec tests.

Gap tests follow the same structure as test-spec tests (`name`, `model`, `given[]`, `expect`). Since there is no ground-truth execution for gap tests, derive `expect.rows` from the model's logic — these are best-effort expectations that `dbt test` will validate.

After adding gap tests, re-run `migrate write` with the updated schema YAML and run:

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt test --select <model_name>
```

If gap tests fail, revise or remove the failing gap test (gap tests are mutable, unlike canonical tests). Do not re-enter the self-correction loop — a single fix attempt is sufficient for gap tests.

## Final Step — Write generate status to catalog

After the dbt model has been created and tested, record the summary in the catalog:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write-catalog \
  --table <fqn> \
  --model-path <relative_path_to_model.sql> \
  --compiled <true|false> \
  --tests-passed <true|false> \
  --test-count <number> \
  --schema-yml <true|false>
```

If there are warnings or errors to report, pass them as JSON arrays:

```bash
  --warnings '[{"code": "...", "message": "..."}]' \
  --errors '[{"code": "...", "message": "..."}]'
```

## Output schemas

| Subcommand | Schema reference |
|---|---|
| `context` | See `docs/design/skill-contract/model-generator.md` section "AssembleContext" |
| `write` | `{ "written": [...], "status": "ok" }` |

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | No profile or no statements. Tell user which prerequisite is missing |
| `migrate context` | 2 | IO/parse error. Surface the error message |
| `migrate write` | 1 | Validation failure (empty SQL). Tell user to regenerate |
| `migrate write` | 2 | IO error (missing dbt project). Tell user to run `/init-dbt` |

## References

- [../_shared/references/sql-style.md](../_shared/references/sql-style.md) — SQL formatting rules with stable codes (SQL_001-SQL_013): keywords, indentation, commas, aliases
- [../_shared/references/cte-structure.md](../_shared/references/cte-structure.md) — CTE pattern rules (CTE_001-CTE_008): import-first order, `final` naming, no nested CTEs
- [../_shared/references/model-naming.md](../_shared/references/model-naming.md) — layer prefix, snake_case, `_dbt_run_id` and `_loaded_at` ETL control column rules (MDL_001-MDL_013)
- [../_shared/references/yaml-style.md](../_shared/references/yaml-style.md) — YAML formatting rules (YML_001-YML_008): `version: 2`, descriptions, indentation
- [references/snapshot-generation.md](references/snapshot-generation.md) — snapshot file placement, strategy selection (timestamp vs check), and config templates
