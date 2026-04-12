# Skill: Generating Model

## Purpose

Generates a dbt model from a profiled stored procedure. Reads deterministic context from catalog (profile, resolved statements, test spec, and refactored SQL), uses LLM to produce dbt-idiomatic SQL following the import CTE / logical CTE / final CTE pattern, validates logical equivalence against the refactored SQL, renders schema YAML with tests, and writes artifacts to the dbt project. Includes a self-correction loop that runs `dbt compile` and `dbt test` up to 3 iterations.

## Invocation

```text
/generating-model <schema.table>
```

Argument is the fully-qualified table name. The writer procedure is read automatically from the catalog scoping section.

Trigger phrases: "migrate a procedure", "generate a dbt model", "convert SP to dbt", "create a model for".

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Command Setup DDL]] first.
- `catalog/tables/<table>.json` must exist with completed scoping and profiling sections.
- A dbt project must exist (`dbt_project.yml` in `./dbt/` relative to project root). If missing, run `/init-dbt` first.
- `test-specs/<item_id>.json` must exist (produced by [[Skill Generating Tests]] and approved by [[Skill Reviewing Tests]]). There is no test-less path -- the test spec is mandatory.

## Pipeline

### 1. Assemble context

```bash
uv run --project <shared-path> migrate context --table <table_fqn>
```

Output JSON contains:

| Field | Description |
|---|---|
| `profile` | Classification, keys, watermark, PII answers |
| `materialization` | Derived from profile (snapshot/table/incremental) |
| `statements` | Resolved statement list with action (migrate/skip) and SQL |
| `proc_body` | Full original procedure SQL |
| `columns` | Target table column list |
| `source_tables` | Tables read by the writer |
| `schema_tests` | Deterministic test specs (entity integrity, referential integrity, recency, PII) |
| `refactored_sql` | Cleaned, CTE-structured SQL produced by the refactor stage |

Use `refactored_sql` as the sole SQL input for generation. `proc_body` and `statements` are ignored during generation -- they exist in context for the reviewer's correctness checks.

### 2. Generate dbt SQL

Generates a single model from `refactored_sql` using `{{ source() }}` directly — no staging models are generated. The full CTE chain is kept inline in one model file.

**Model (`<target_table>.sql`)**:

```sql
{{ config(
    materialized='<materialization>'
) }}

with <source_table> as (
    select * from {{ source('<schema>', '<table>') }}
),

<logical_cte> as (
    ...
),

final as (
    ...
)

select * from final
```

Style guides applied throughout:

- `sql-style.md` -- keywords, indentation, commas, aliases (SQL_001--SQL_013)
- `cte-structure.md` -- import-first order, `final` naming, no nested CTEs (CTE_001--CTE_008)
- `model-naming.md` -- layer prefixes, `_dbt_run_id`, `_loaded_at` rules (MDL_001--MDL_013)

**Materialization mapping from classification:**

| Classification | Materialization | Additional config |
|---|---|---|
| `dim_scd2` | `snapshot` | `strategy='timestamp'`, `updated_at='<watermark>'`, `unique_key='<pk>'` |
| `dim_scd1` | `incremental` | `incremental_strategy='merge'`, `unique_key='<pk>'` |
| `dim_non_scd` | `table` | -- |
| `dim_junk` | `table` | -- |
| `fact_transaction` | `incremental` | Append strategy with watermark filter |
| `fact_periodic_snapshot` | `table` | -- |
| `fact_accumulating_snapshot` | `incremental` | `incremental_strategy='merge'`, `unique_key='<pk>'` |
| `fact_aggregate` | `table` | -- |

For incremental models, the watermark filter is added in a logical CTE:

```sql
{% if is_incremental() %}
where <watermark_column> > (select max(<watermark_column>) from {{ this }})
{% endif %}
```

For snapshot models, a dbt snapshot block is generated in `snapshots/` instead of `models/staging/`.

### 4. Logical equivalence check

Compares generated model against `refactored_sql`:

| Check | What is compared |
|---|---|
| Source tables | Same tables read in model vs refactored SQL |
| Columns selected | Same columns in final SELECT vs original INSERT column list |
| Join conditions | Same join keys and join types (INNER/LEFT/RIGHT/FULL) |
| Filter predicates | Same WHERE/HAVING conditions (modulo syntax) |
| Aggregation grain | Same GROUP BY columns |
| Write semantics | INSERT/MERGE/UPDATE intent preserved by materialization |

Results:

- **Match** -- proceed silently
- **Intentional divergence** (e.g., ISNULL -> COALESCE) -- noted as informational
- **Semantic gap** (missing join, different grain, dropped column) -- flagged as warning, presented to user

### 5. Build schema.yml

**5a -- Schema tests** from `schema_tests` in context:

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

Apply `yaml-style.md` (YML_001--YML_008) throughout.

**5b -- Render test-spec unit tests** from `test-specs/<item_id>.json`:

Every `unit_tests[]` entry is rendered into a `unit_tests:` block in the schema YAML. These ground-truth tests are immutable and must not be dropped or modified. Gap tests (`test_gap_*`) from Step 5c, however, may be revised during the self-correction loop (Step 8c) if their best-effort expectations prove incorrect.

```yaml
    unit_tests:
      - name: test_merge_matched_existing_product_updated
        model: stg_dimproduct
        given:
          - input: source('bronze', 'product')
            rows:
              - { product_id: 1, product_name: "Widget", list_price: 99.99 }
        expect:
          rows:
            - { product_key: 1, product_name: "Widget", list_price: 99.99 }
```

**5c -- Coverage gap tests:**

After rendering test-spec tests, the model's logic is analyzed for uncovered branches. 1-3 additional `test_gap_*` scenarios are generated with best-effort expectations derived from model logic.

### 7. Write artifacts

```bash
mkdir -p .staging
uv run --project <shared-path> migrate write \
  --table <table_fqn> \
  --model-sql-file .staging/model.sql \
  --schema-yml-file .staging/schema.yml; rm -rf .staging
```

The dbt project path is resolved from `$DBT_PROJECT_PATH` or defaults to `./dbt`.

Artifact write paths:

- Model SQL: `dbt/models/staging/<model_name>.sql`
- Schema YAML: `dbt/models/staging/_<model_name>.yml`
- Snapshot SQL: `dbt/snapshots/` for snapshot targets

### 8. Compile and test

**8a -- Compile:**

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt compile --select <model_name>
```

If compile fails with a connection error, falls back to `dbt parse` for offline validation.

**8b -- Run unit tests:**

```bash
cd "${DBT_PROJECT_PATH:-./dbt}" && dbt test --select <model_name>
```

**8c -- Self-correction loop (max 3 iterations):**

If compile or test fails:

1. Analyze failure output
2. Revise model SQL (test-spec unit tests are ground truth and cannot be modified; `test_gap_*` tests may be revised)
3. Re-run `migrate write`, `dbt compile`, `dbt test`
4. Repeat up to 3 iterations

After 3 failed iterations, the model is left as-is with `status: "partial"` and failures recorded in `execution.dbt_errors[]`.

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<table>.json` | Profile, scoping, columns |
| `catalog/procedures/<writer>.json` | Statements, refs, proc body |
| `test-specs/<item_id>.json` | Approved test scenarios with ground truth |
| `dbt_project.yml` | dbt project configuration |

## Writes

### Model SQL file

Written to `dbt/models/staging/<model_name>.sql`.

### Schema YAML file

Written alongside the mart model.

### Snapshot files

For snapshot models, generated in `dbt/snapshots/` instead of `dbt/models/`.

### Migration artifact manifest (`migration_artifact_manifest.json`)

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Always `"1.0"` |
| `run_id` | string | UUID for the migration run |
| `results[]` | array | Per-item migration results |
| `results[].item_id` | string | Table FQN |
| `results[].status` | string | Enum: `ok`, `partial`, `error` |
| `results[].output.table_ref` | string | Target table reference |
| `results[].output.model_name` | string | Generated dbt model name |
| `results[].output.artifact_paths` | object | `model_sql`, `model_yaml` paths |
| `results[].output.generated.model_sql` | object | `materialized` (enum: `incremental`, `table`, `snapshot`), `uses_watermark` |
| `results[].output.generated.model_yaml` | object | `has_model_description`, `schema_tests_rendered[]`, `has_unit_tests` |
| `results[].output.execution` | object | `dbt_compile_passed`, `dbt_test_passed`, `self_correction_iterations`, `dbt_errors[]` |

## JSON Format

### Generated model SQL example

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_key',
    incremental_strategy='merge'
) }}

with source_customers as (
    select * from {{ source('bronze', 'customer') }}
),

source_geography as (
    select * from {{ source('bronze', 'geography') }}
),

customers_with_region as (
    select
        c.customer_key,
        c.first_name,
        c.last_name,
        g.country as region
    from source_customers c
    left join source_geography g
        on c.geography_key = g.geography_key
    {% if is_incremental() %}
    where c.modified_date > (select max(modified_date) from {{ this }})
    {% endif %}
),

final as (
    select
        customer_key,
        first_name,
        last_name,
        region,
        current_timestamp() as _loaded_at
    from customers_with_region
)

select * from final
```

### Schema YAML example

```yaml
version: 2

models:
  - name: stg_dimcustomer
    description: "Migrated from silver.usp_load_dimcustomer. Target: silver.DimCustomer."
    columns:
      - name: customer_key
        description: "Primary key (surrogate)"
        data_tests:
          - unique
          - not_null
      - name: region
        description: "Foreign key to DimGeography"
        data_tests:
          - relationships:
              to: ref('dimgeography')
              field: geography_key
      - name: first_name
        meta:
          contains_pii: true
          pii_action: mask
    unit_tests:
      - name: test_merge_matched_existing_customer_updated
        model: stg_dimcustomer
        given:
          - input: source('bronze', 'customer')
            rows:
              - { customer_key: 1, first_name: "Jane", last_name: "Doe", modified_date: "2024-06-01" }
        expect:
          rows:
            - { customer_key: 1, first_name: "Jane", last_name: "Doe", region: "US" }
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `migrate context` exit code 1 | No profile or no statements in catalog | Run [[Skill Analyzing Table]], [[Skill Profiling Table]], and [[Skill Generating Tests]] first |
| `migrate context` exit code 2 | IO/parse error | Check file permissions and JSON validity in `catalog/` |
| `migrate write` exit code 1 | Validation failure (empty SQL) | Regenerate the model |
| `migrate write` exit code 2 | IO error (missing dbt project) | Run `/init-dbt` to create the dbt project |
| `dbt compile` connection error | No warehouse connection available | Falls back to `dbt parse` for offline validation. Unit tests are skipped |
| `dbt test` failure after 3 iterations | Model cannot pass all unit tests | Model left with `status: "partial"`. Failing tests recorded in `execution.dbt_errors[]`. Manual intervention needed |
| Equivalence check warnings | Generated model diverges from refactored SQL semantics | Review warnings with user. Revise model if semantic gaps are real |
| Test-spec scenario missing from YAML | Scenario dropped during rendering | All `unit_tests[]` from test spec must be preserved -- re-run rendering |
