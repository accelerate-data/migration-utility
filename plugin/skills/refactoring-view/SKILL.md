---
name: refactoring-view
description: >
  Refactors a SQL Server view into an ephemeral dbt staging model with an
  import/logical/final CTE structure. Reads view DDL via the DDL MCP server,
  restructures into dbt Jinja SQL, and writes stg_<view_name>.sql to
  dbt/models/staging/. Invoked by /refactor-view per dependent view in
  dependency order.
user-invocable: false
argument-hint: "<schema.view_name>"
---

# Refactoring View

Restructure a SQL Server view into an ephemeral dbt staging model using the import/logical/final CTE pattern. The output is written to `dbt/models/staging/stg_<view_name>.sql`.

## Arguments

`$ARGUMENTS` is the fully-qualified view name (e.g. `silver.vw_customer_dim`). The view catalog at `catalog/views/<schema.view_name>.json` must already exist.

## Step 1: Read view DDL and catalog

Read the view DDL via the DDL MCP server:

```text
list_views → confirm the view exists
get_view_body(<schema.view_name>) → raw CREATE VIEW DDL
```

Also read the view catalog:

```text
catalog/views/<schema.view_name>.json
```

The catalog provides:

- `columns[]` — resolved column names and types (from `sys.columns`)
- `references.tables.in_scope[]` — source tables the view reads from
- `references.views.in_scope[]` — other views this view depends on (already migrated as `stg_` models before this skill runs)

## Step 2: Determine source references

For each entry in `references.tables.in_scope`:

- It becomes a dbt `source()` reference: `{{ source('<schema>', '<table_name>') }}`

For each entry in `references.views.in_scope`:

- It becomes a dbt `ref()` reference: `{{ ref('stg_<view_name>') }}`

## Step 3: Generate the dbt staging model

Generate `stg_<view_name>.sql` using the following structure:

```sql
{{ config(materialized='ephemeral') }}

-- import CTEs: one per source table or dependent view
with <source_alias> as (
    select * from {{ source('<schema>', '<table_name>') }}
),

-- logical CTEs: one transformation step per CTE
<logical_cte_name> as (
    select
        <columns>
    from <source_alias>
    <joins, filters, aggregations>
),

-- final CTE: assembles the output column list
final as (
    select
        <column_list matching catalog.columns>
    from <logical_cte_name>
)

select * from final
```

Rules:

- One import CTE per source table or dependent view — `SELECT *` from the source reference.
- Logical CTEs mirror the view's transformation steps (joins, filters, computed columns, aggregates). Name them descriptively.
- The `final` CTE produces the exact column list from the original view's SELECT (use `catalog.columns` as the reference for expected output columns).
- Use dbt Jinja syntax for all source and ref references — no bare table names.
- Use standard SQL syntax (not T-SQL): avoid `ISNULL` (use `COALESCE`), avoid `CONVERT` (use `CAST`), avoid `TOP` (use `LIMIT`).
- Flatten subqueries into sequential CTEs.
- Replace any temp table references with logical CTEs.

## Step 4: Write the model file

Write the generated SQL to:

```text
dbt/models/staging/stg_<view_name>.sql
```

Where `<view_name>` is the unqualified view name (no schema prefix), lowercased.

Create the directory if it does not exist.

## Step 5: Report result

Return a result JSON in the item result schema expected by `/refactor-view`:

```json
{
  "item_id": "<schema.view_name>",
  "status": "ok",
  "output": {
    "cte_count": 3,
    "import_ctes": ["source_customers"],
    "logical_ctes": ["customers_with_region"],
    "final_cte": "final",
    "columns_checked": 0,
    "columns_missing": 0,
    "dbt_compile": "pending",
    "dbt_test": "pending"
  },
  "warnings": [],
  "errors": []
}
```

Set `status` to `"error"` if the view DDL could not be read or the model could not be generated. Set `status` to `"partial"` if the model was generated but some columns from the original view could not be mapped.

## Error handling

| Situation | Action |
|---|---|
| `get_view_body` returns "not found" | Set status `error`, code `VIEW_CATALOG_MISSING` |
| View DDL is a `WITH CHECK OPTION` or security wrapper only | Generate a pass-through CTE model with a comment explaining the view is a security wrapper |
| Source table not in `references.tables` (unresolved reference) | Include the table name as a bare reference with a `-- TODO: add to sources.yml` comment; set status `partial` |
| Cannot determine column list from DDL | Use `SELECT *` in final CTE with a warning; set status `partial` |
