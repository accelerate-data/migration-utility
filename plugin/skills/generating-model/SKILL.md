---
name: generating-model
description: >
  Generates a dbt model from a stored procedure. Invoke when the user asks to
  "migrate a procedure", "generate a dbt model", "convert SP to dbt", or
  "create a model for <table>". Requires catalog profile, resolved
  statements from prior discover + profile stages, and an approved test spec
  from the test-generation stage.
user-invocable: true
argument-hint: "<schema.table>"
---

# Generating Model

Generate a dbt model from a profiled stored procedure. Reads deterministic context from catalog, uses LLM to produce dbt-idiomatic SQL, validates logical equivalence, and writes artifacts to the dbt project. The reviewer will check the output against the same reference files loaded above.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` → `scoping.selected_writer`).

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <table_fqn> generating-model
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <table_fqn>
```

The CLI reads the selected writer from the table's catalog scoping section — no `--writer` argument needed.

Read the output JSON. It contains:

- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived from profile (snapshot/table/incremental)
- `statements` — resolved statement list with action (migrate/skip) and SQL
- `proc_body` — full original procedure SQL
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `schema_tests` — deterministic test specs (entity integrity, referential integrity, recency, PII)
- `refactored_sql` — cleaned, CTE-structured SQL produced by the refactor stage

Use `refactored_sql` as your sole SQL input. Ignore `proc_body` and `statements` — they are not relevant to model generation.

## Step 1.5: Pre-generation check

Check whether the model sweep plan and existing dbt artifacts indicate this table should be skipped or run in test-only mode.

### 1.5a — Read the plan artifact

If a model sweep artifact path was provided (via `--model-sweep-file` argument or referenced in the agent prompt), read `.migration-runs/model-sweep.<epoch>.json`. Find this table's entry by matching `fqn`.

If no sweep artifact is available (single-table interactive run), skip to step 1.5c.

### 1.5b — Act on recommended_action

| `recommended_action` | Action |
|---|---|
| `"skip"` | Report "Model exists with passing tests — skipping generation." Write item result with `status: "ok"` and `"skipped": true`. Stop — do not proceed to Step 2. |
| `"test-only"` | Skip Steps 2–7. Jump directly to Step 8 (compile + test) using the existing mart model on disk. |
| `"generate"` | Proceed normally through all steps. |

### 1.5c — Shared staging models

When proceeding with `"generate"`:

- Check `shared_staging_candidates` in the sweep artifact. For any source table listed there, do **not** create a new `stg_*` file — it was already written by the planning sweep. Use `{{ ref('stg_<table>') }}` in the mart model.
- For source tables not in `shared_staging_candidates`, apply the normal check: look for an existing `stg_*.sql` in `dbt/models/staging/`. If one exists with a compatible column set, use `{{ ref() }}` — do not duplicate. If no stg file exists, you will create one in Step 3.

**Rule:** mart models must never use `{{ source() }}` directly. Every source table read in the mart must go through a `stg_*` model (existing or newly created) referenced via `{{ ref() }}`.

## Step 2: Decide model structure

The refactored SQL already has import/logical/final CTE structure. Apply the staging/mart split — see [modular-modeling-ref.md](references/modular-modeling-ref.md) for decision rules: import CTEs → ephemeral `stg_*` models, logical+final CTEs → mart model.

Before creating a new `stg_*` model, check `dbt/models/staging/` for an existing one on the same source table. If it exists and its column set is compatible, use `{{ ref() }}` — do not duplicate.

## Step 3: Generate dbt SQL

Produce two outputs from the `refactored_sql`. Apply [sql-style.md](../reviewing-model/references/sql-style.md) (keywords, indentation, commas) and [cte-structure.md](../reviewing-model/references/cte-structure.md) (import/logical/final pattern) throughout. Apply [model-naming.md](../reviewing-model/references/model-naming.md) for layer prefixes, `_dbt_run_id`, and `_loaded_at` rules.

**Mandatory pre-step — write stg files before any mart SQL:**

From the `migrate context` output, read `source_tables`. For each source table where `is_selected=true` and `is_updated=false`:

- Check whether `dbt/models/staging/stg_<table>.sql` exists on disk.
- If it does not exist, use the Write tool to create it now as an ephemeral model: `{{ config(materialized='ephemeral') }}` + `select * from {{ source('<schema>', '<table>') }}`.

Only after all stg files are confirmed on disk (existing or newly written), generate the mart SQL. The mart must use `{{ ref('stg_<table>') }}` — never `{{ source() }}` — for each source table.

### Staging models (`stg_<source_table>.sql`)

One staging model per import CTE in the refactored SQL. Each is `materialized='ephemeral'` and does `select * from {{ source('<schema>', '<table>') }}` with light transforms only.

```sql
{{ config(materialized='ephemeral') }}

select * from {{ source('<schema>', '<table>') }}
```

### Mart model (`<target_table>.sql`)

The mart model replaces import CTEs with `{{ ref('stg_...') }}` calls; logical and final CTEs stay as-is. The config block uses the profile-derived materialization.

```sql
{{ config(
    materialized='<materialization>'
) }}

with <source_table> as (
    select * from {{ ref('stg_<source_table>') }}
),

<logical_cte> as (
    ...
),

final as (
    ...
)

select * from final
```

For incremental models, add the watermark to the config:

```sql
{{ config(
    materialized='incremental',
    unique_key='<pk_column>',
    incremental_strategy='merge'
) }}
```

And add the incremental filter in the appropriate logical CTE:

```sql
{% if is_incremental() %}
where <watermark_column> > (select max(<watermark_column>) from {{ this }})
{% endif %}
```

For snapshot models, generate a dbt snapshot block instead of a model file. Place the file in `snapshots/` (not `models/staging/`). The snapshot file replaces the `.sql` model — do not generate both.

**When the profile has a watermark column** — use `strategy='timestamp'`:

```sql
{% snapshot <model_name>_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='<pk_column>',
    strategy='timestamp',
    updated_at='<watermark_column>',
) }}

select * from {{ source('<source_name>', '<table_name>') }}

{% endsnapshot %}
```

**When the profile has no watermark column** — use `strategy='check'`:

```sql
{% snapshot <model_name>_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='<pk_column>',
    strategy='check',
    check_cols='all',
) }}

select * from {{ source('<source_name>', '<table_name>') }}

{% endsnapshot %}
```

Use a specific column list for `check_cols` if the profile identifies mutable columns.

## Step 4: Logical equivalence check

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
- **Intentional divergence** (e.g., T-SQL `ISNULL` replaced with `COALESCE`): note as informational
- **Semantic gap** (missing join, different grain, dropped column): flag as **warning** and present to user

If warnings exist, present them before proceeding:

```text
Equivalence check found 2 warnings:

  1. [columns] Original INSERT includes column 'legacy_flag' not in generated model
  2. [filter] Original WHERE clause has 'IsActive = 1' not present in generated model

Proceed with these differences? (y/n)
```

Ask the user and wait. If the user says no, revise the model.

## Step 5: Build schema.yml

Apply [yaml-style.md](../reviewing-model/references/yaml-style.md) (indentation, `version: 2`, required descriptions) throughout.

### 5a — Schema tests

Render `schema_tests` from context into the `columns:` section of the schema YAML:

```yaml
version: 2

models:
  - name: <model_name>
    description: "Migrated from <writer_fqn>. Target: <table_fqn>."
    columns:
      - name: <pk_column>
        description: "Primary key"
        data_tests:
          - unique
          - not_null
      - name: <fk_column>
        description: "Foreign key to <ref_table>"
        data_tests:
          - relationships:
              to: ref('<ref_model>')
              field: <ref_column>
      - name: <pii_column>
        meta:
          contains_pii: true
          pii_action: <action>
```

Include `recency` test for incremental models if watermark is present.

### 5b — Render test-spec unit tests

Read `test-specs/<item_id>.json` and render every entry in `unit_tests[]` into a `unit_tests:` block in the schema YAML, at the same level as `columns:` under the model. Preserve every scenario exactly — none may be dropped or modified.

```yaml
    unit_tests:
      - name: test_merge_matched_existing_product_updated
        model: stg_dimproduct
        given:
          - input: source('bronze', 'product')
            rows:
              - { product_id: 1, product_name: "Widget", list_price: 99.99 }
          - input: ref('stg_dimproduct')
            rows:
              - { product_key: 1, product_name: "Old Widget", list_price: 50.00 }
        expect:
          rows:
            - { product_key: 1, product_name: "Widget", list_price: 99.99 }
```

### 5c — Identify coverage gaps and create additional tests

After rendering the test-spec's unit tests, analyze the generated model's logic for branches not covered by existing scenarios. Look for:

- JOIN conditions with no matching/non-matching test case
- CASE/WHEN arms not exercised
- NULL handling paths (COALESCE, ISNULL replacements)
- Incremental filter (`is_incremental()`) boundary cases
- Empty source table edge cases

Generate 1-3 additional unit test scenarios for uncovered branches. Add them to the `unit_tests:` block alongside the test-spec scenarios. Use the naming convention `test_gap_<description>` to distinguish LLM-generated tests from ground-truth test-spec tests.

Gap tests follow the same structure as test-spec tests (`name`, `model`, `given[]`, `expect`). Since there is no ground-truth execution for gap tests, derive `expect.rows` from the model's logic — these are best-effort expectations that `dbt test` will validate.

## Step 6: Present for approval

Show the user:

1. Generated model SQL (full file)
2. Schema YAML (full file)
3. Equivalence check results (if any warnings)
4. Materialization and config decisions

Ask the user: "Approve this model to write to the dbt project? (y/n/edit)". If the user requests edits, apply them and re-run the equivalence check on the edited version.

## Step 7: Write artifacts

After approval:

Write the generated SQL and YAML to temporary files first to avoid shell escaping issues with multi-line content:

1. Write the model SQL to `.staging/model.sql`
2. Write the schema YAML to `.staging/schema.yml`

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate write \
  --table <table_fqn> \
  --model-sql-file .staging/model.sql \
  --schema-yml-file .staging/schema.yml; rm -rf .staging
```

The dbt project path is resolved automatically from `$DBT_PROJECT_PATH` or defaults to `./dbt` relative to the project root. Pass `--dbt-project-path <path>` only if you need to override this.

Report the written file paths to the user.

## Step 8: Compile and test

### 8a — Compile

Run `dbt compile` to verify the generated model compiles:

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt compile --select <model_name>
```

If compile fails with a **connection error** (adapter cannot reach the warehouse — look for "Could not connect", "Login failed", "Connection refused", or similar adapter errors):

1. Tell the user: "No warehouse connection available. Falling back to offline validation."
2. Run `dbt parse` in the dbt project directory instead.
3. Report parse results. If parse fails, attempt to fix (max 3 iterations as below). Skip `dbt test` — unit tests require compilation.

If compile fails with a **non-connection error** (syntax, bad ref, macro resolution), proceed to the self-correction loop in 8c.

### 8b — Run unit tests

On compile success, run unit tests:

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt test --select <model_name>
```

If all tests pass, report success and proceed to the next step.

### 8c — Self-correction loop (max 3 iterations)

If compile or test fails:

1. Analyze the failure output — identify which test failed and why (wrong column, missing row, type mismatch, etc.).
2. Revise the model SQL to fix the issue. Do not modify test-spec unit tests — they are ground truth. Gap tests (`test_gap_*`) may be revised if their expectations were incorrect.
3. Re-run `migrate write` with the revised SQL and schema YAML.
4. Re-run `dbt compile` and `dbt test`.
5. Repeat up to 3 iterations total.

After 3 failed iterations:

- Report the failing test names and error details to the user.
- Leave the model as-is with `status: "partial"`.
- Record failures in `execution.dbt_errors[]`.

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

- [references/modular-modeling-ref.md](references/modular-modeling-ref.md) — staging/mart split decision rules, file layout, and CTE mapping
- [../reviewing-model/references/sql-style.md](../reviewing-model/references/sql-style.md) — SQL formatting rules with stable codes (SQL_001–SQL_013): keywords, indentation, commas, aliases
- [../reviewing-model/references/cte-structure.md](../reviewing-model/references/cte-structure.md) — CTE pattern rules (CTE_001–CTE_008): import-first order, `final` naming, no nested CTEs
- [../reviewing-model/references/model-naming.md](../reviewing-model/references/model-naming.md) — layer prefix, snake_case, `_dbt_run_id` and `_loaded_at` ETL control column rules (MDL_001–MDL_013)
- [../reviewing-model/references/yaml-style.md](../reviewing-model/references/yaml-style.md) — YAML formatting rules (YML_001–YML_008): `version: 2`, descriptions, indentation
