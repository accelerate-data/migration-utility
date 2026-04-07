---
name: analyzing-view
description: >
  SQL structure analysis and catalog persistence for a single view or materialized view. Fetches view DDL via discover show, extracts SQL elements (JOINs, aggregations, window functions, CASE, subqueries, CTEs), builds a call tree, generates a logic summary, and persists scoping results to the view catalog file.
user-invocable: true
argument-hint: "<schema.view_name>"
---

# Analyzing View

Analyze a view or materialized view's SQL structure and persist the findings to the view catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified view name (e.g. `silver.vw_CustomerSales`, `[dbo].[vw_FactSales]`). Ask the user if missing.

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <view_fqn> scope
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Pipeline

### Step 1 -- Show view from catalog

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

This returns:

- `raw_ddl` — the full CREATE VIEW DDL text
- `refs` — `reads_from` (source tables) and any view refs
- `sql_elements` — SQLglot-extracted SQL features (JOINs, aggregations, etc.). May be `null` if `needs_llm` is true.
- `needs_llm` — true when SQLglot could not parse the DDL
- `errors` — any parse errors

Read `catalog/views/<view_fqn>.json` to get `is_materialized_view` and `references.views.in_scope`.

Present the object type and, for materialized views, column count:

```text
silver.vw_CustomerSales (view)
```

or

```text
silver.mv_SalesSummary (materialized view, 8 columns)
```

If `errors` contains `DDL_PARSE_ERROR` and `needs_llm` is true, note that SQLglot could not parse the DDL and proceed using `raw_ddl` directly for Steps 2-3.

### Step 2 -- Build call tree

Resolve sources from `refs.reads_from` (source tables) and `references.views.in_scope` from the view catalog (source views).

```text
Call tree for silver.vw_CustomerSales:

  Reads tables:  bronze.Customer, bronze.Person
  Reads views:   silver.vw_AddressBase
```

If `references.views.in_scope` is non-empty, add a `VIEW_DEPENDS_ON_VIEWS` warning to the scoping output (see Step 5).

### Step 3 -- Identify SQL elements

If `needs_llm` is false and `sql_elements` is populated, present them directly:

```text
SQL elements:
  - join: INNER JOIN bronze.Person
  - join: LEFT JOIN bronze.Address
  - group_by: GROUP BY
  - aggregation: SUM, COUNT
```

If `needs_llm` is true, read `raw_ddl` and identify SQL features manually: JOINs (type and target), GROUP BY, aggregation functions, window functions (OVER), CASE expressions, subqueries, CTEs. Present the same format as above.

### Step 4 -- Logic summary

Read `raw_ddl` and write a plain-language description of what the view computes (2-4 sentences). Focus on:

- What data sources are combined
- What transformations are applied (filtering, joining, aggregating)
- What the view produces

### Step 5 -- Present and confirm

Present findings to the user and wait for explicit confirmation before persisting:

```text
Analysis of silver.vw_CustomerSales

Call tree:
  Reads tables: bronze.Customer, bronze.Person
  Reads views:  (none)

SQL elements:
  - join: INNER JOIN bronze.Person on CustomerKey
  - aggregation: COUNT

Logic summary:
  Joins customer records with person details on CustomerKey. Counts
  the number of persons per customer. Produces one row per customer
  with an enriched name and person count.

⚠ No warnings.

Persist this analysis to catalog/views/silver.vw_customersales.json? (y/n)
```

If the view depends on other views, show the warning prominently:

```text
⚠ VIEW_DEPENDS_ON_VIEWS: silver.vw_addressbase has not been analyzed yet.
  Run /scope silver.vw_addressbase first for accurate profiling-view results.
```

### Step 6 -- Persist scoping to catalog

Write the scoping JSON to a temp file to avoid shell quoting issues:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <view_fqn> --scoping-file .staging/scoping.json; rm -rf .staging
```

The scoping JSON shape:

```json
{
  "status": "analyzed",
  "sql_elements": [
    {"type": "join", "detail": "INNER JOIN bronze.person"},
    {"type": "aggregation", "detail": "COUNT"}
  ],
  "call_tree": {
    "reads_from": ["bronze.customer", "bronze.person"],
    "views_referenced": []
  },
  "logic_summary": "...",
  "rationale": "...",
  "warnings": [],
  "errors": []
}
```

Include `VIEW_DEPENDS_ON_VIEWS` in `warnings` if applicable:

```json
{"code": "VIEW_DEPENDS_ON_VIEWS", "severity": "warning", "message": "View references unanalyzed views: silver.vw_addressbase. Classify dependencies before running profiling-view."}
```

If `discover write-scoping` exits non-zero, report the error to the user. Do not retry automatically.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover show` | 1 | Object not found or catalog file missing. Report and stop |
| `discover show` | 2 | IO error. Report and stop |
| `discover show` | 0 + `needs_llm: true` | Use `raw_ddl` for Steps 3-4. Include `DDL_PARSE_ERROR` in scoping errors. |
| `discover write-scoping` | 1 | Validation failure. Report and stop |
| `discover write-scoping` | 2 | IO error. Report and stop |
