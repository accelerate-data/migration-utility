---
name: migrate-table
description: >
  Generates a dbt model from a stored procedure. Invoke when the user asks to
  "migrate a procedure", "generate a dbt model", "convert SP to dbt", or
  "create a model for <table>". Requires catalog profile and resolved
  statements from prior discover + profile stages.
user-invocable: true
argument-hint: "[--table <fqn>] [--writer <fqn>]"
---

# Migrate

Generate a dbt model from a profiled stored procedure. Reads deterministic context from catalog, uses LLM to produce dbt-idiomatic SQL, validates logical equivalence, and writes artifacts to the dbt project.

## Arguments

Parse `$ARGUMENTS` for `--table` and `--writer`. Use `AskUserQuestion` if either is missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, stop and tell the user to run `setup-ddl` first.
2. Confirm a dbt project exists (look for `dbt_project.yml` in `./dbt/` relative to the project root or ask the user for `--dbt-project-path`). If missing, tell the user to run `/init-dbt` first.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../lib" migrate context \
  --table <table_fqn> --writer <writer_fqn>
```

Read the output JSON. It contains:

- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived from profile (snapshot/table/incremental)
- `statements` — resolved statement list with action (migrate/skip) and SQL
- `proc_body` — full original procedure SQL
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `schema_tests` — deterministic test specs (entity integrity, referential integrity, recency, PII)

## Step 2: Decide model structure

Analyse the `statements` array and `proc_body` to determine the model structure:

| Pattern | Model structure |
|---|---|
| Single INSERT from source tables | One staging model |
| Multiple INSERTs to the same target table | One model with UNION ALL |
| Multiple INSERTs to different target tables | Separate models (one per target) |
| Temp table chains (INSERT INTO #temp, then INSERT from #temp) | Staging + intermediate models with `{{ ref() }}` |
| Nested subqueries in SELECT | Flatten into sequential CTEs |
| MERGE with complex USING clause | Single model; MERGE semantics become incremental config |

## Step 3: Generate dbt SQL

Follow the **import CTE -> logical CTE -> final CTE** pattern:

### Import CTEs

All external data sources at the top of the model, each in its own CTE:

```sql
with source_customers as (
    select * from {{ source('bronze', 'customer') }}
),

dim_product as (
    select * from {{ ref('stg_dim_product') }}
),
```

Rules for import CTEs:

- Tables in `source_tables` that are raw/bronze use `{{ source('<schema>', '<table>') }}`
- Tables that are already dbt models (silver/gold, or previously migrated) use `{{ ref('<model_name>') }}`
- Name the CTE after the source table (descriptive, not `cte1` or `temp`)
- One CTE per source — never combine multiple sources in one import CTE

### Logical CTEs

One transformation step per CTE, named descriptively:

```sql
customers_with_region as (
    select
        c.customer_key,
        c.first_name,
        g.country as region
    from source_customers c
    left join source_geography g
        on c.customer_key = g.customer_key
),

filtered_customers as (
    select *
    from customers_with_region
    where region is not null
),
```

Rules for logical CTEs:

- Each CTE does one thing — join, filter, aggregate, or transform
- CTE names describe the transformation, not `step1`/`step2`
- Preserve the original SQL semantics (same joins, filters, aggregations)
- Replace T-SQL syntax with ANSI SQL / Spark SQL equivalents
- Replace procedure parameters with `{{ var('param_name', 'default_value') }}`

### Final CTE and SELECT

```sql
final as (
    select
        customer_key,
        first_name,
        region,
        current_timestamp() as _loaded_at
    from filtered_customers
)

select * from final
```

### Config block

Add `{{ config() }}` at the top of the model:

```sql
{{ config(
    materialized='<materialization>'
) }}
```

For incremental models, add the watermark:

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

For snapshot models, use the dbt snapshot block pattern instead of a model.

## Step 4: Logical equivalence check

Compare the generated model against the original `migrate` statements. Check each of these:

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

Use `AskUserQuestion` and wait. If the user says no, revise the model.

## Step 5: Build schema.yml

Render `schema_tests` from context into a dbt schema YAML:

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

## Step 6: Present for approval

Show the user:

1. Generated model SQL (full file)
2. Schema YAML (full file)
3. Equivalence check results (if any warnings)
4. Materialization and config decisions

Use `AskUserQuestion`: "Approve this model to write to the dbt project? (y/n/edit)"

If the user requests edits, apply them and re-run the equivalence check on the edited version.

## Step 7: Write artifacts

After approval:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../lib" migrate write \
  --table <table_fqn> \
  --dbt-project-path <dbt-project-path> \
  --model-sql '<generated_sql>' \
  --schema-yml '<generated_yml>'
```

Report the written file paths to the user.

## Output schemas

| Subcommand | Schema reference |
|---|---|
| `context` | See `docs/design/sp-to-dbt-plugin/README.md` section "migrate context" |
| `write` | `{ "written": [...], "status": "ok" }` |

## Error handling

- `migrate context` exits 1 if table has no profile or writer has no statements — tell user which prerequisite is missing
- `migrate context` exits 2 on IO/parse errors — surface the error message
- `migrate write` exits 1 on validation failure (empty SQL) — tell user to regenerate
- `migrate write` exits 2 on IO error (missing dbt project) — tell user to run `/init-dbt`
