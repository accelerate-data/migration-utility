---
name: analyzing-table
description: >
  Scoping for a single table, view, or materialized view. For tables: discovers writer procedures, analyzes candidates, resolves the selected writer. For views/MVs: extracts SQL elements, builds call tree, generates logic summary. Auto-detects object type from catalog presence.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Scoping

Scope a table, view, or materialized view and persist the scoping decision to the catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified name (e.g. `silver.DimCustomer`, `silver.vw_CustomerSales`). Ask the user if missing.

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <fqn> scope
```

If the FQN is a view (check `catalog/views/<fqn>.json` existence), use the `scope-view` guard set instead:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <fqn> scope-view
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:
- **If yes** → this is a **view or MV**. Follow the **View Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Pipeline** below.

---

## View Pipeline

Follow these steps when the FQN refers to a view or materialized view.

### Step V1 -- Show view from catalog

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

This returns:

- `raw_ddl` — the full CREATE VIEW DDL text
- `refs` — `reads_from` (source tables) and any view refs
- `sql_elements` — SQLglot-extracted SQL features (JOINs, aggregations, etc.). `null` when `errors` contains `DDL_PARSE_ERROR`.
- `errors` — any parse errors (e.g. `DDL_PARSE_ERROR` when SQLglot could not parse the DDL)

Read `catalog/views/<view_fqn>.json` to get `is_materialized_view` and `references.views.in_scope`.

Present the object type and, for materialized views, column count:

```text
silver.vw_CustomerSales (view)
```

If `errors` contains `DDL_PARSE_ERROR` (i.e. `sql_elements` is null), note that SQLglot could not parse the DDL and proceed using `raw_ddl` directly for Steps V2-V3.

### Step V2 -- Build call tree

Resolve sources from `refs.reads_from` (source tables) and `references.views.in_scope` from the view catalog (source views).

```text
Call tree for silver.vw_CustomerSales:

  Reads tables:  bronze.Customer, bronze.Person
  Reads views:   silver.vw_AddressBase
```

If `references.views.in_scope` is non-empty, add a `VIEW_DEPENDS_ON_VIEWS` warning to the scoping output (see Step V5).

### Step V3 -- Identify SQL elements

If `sql_elements` is populated, present them directly:

```text
SQL elements:
  - join: INNER JOIN bronze.Person
  - join: LEFT JOIN bronze.Address
  - group_by: GROUP BY
  - aggregation: SUM, COUNT
```

If `sql_elements` is null (parse error), read `raw_ddl` and identify SQL features manually: JOINs (type and target), GROUP BY, aggregation functions, window functions (OVER), CASE expressions, subqueries, CTEs. Present the same format as above.

### Step V4 -- Logic summary

Read `raw_ddl` and write a plain-language description of what the view computes (2-4 sentences). Focus on:

- What data sources are combined
- What transformations are applied (filtering, joining, aggregating)
- What the view produces

### Step V5 -- Present and confirm

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

Persist this analysis to catalog/views/silver.vw_customersales.json? (y/n)
```

If the view depends on other views, show the warning prominently:

```text
⚠ VIEW_DEPENDS_ON_VIEWS: silver.vw_addressbase has not been analyzed yet.
  Run /scope silver.vw_addressbase first for accurate profiling results.
```

### Step V6 -- Persist scoping to catalog

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

Include `VIEW_DEPENDS_ON_VIEWS` in `warnings` if applicable.

---

## Table Pipeline

Follow these steps when the FQN refers to a table.

### Step 1 -- Show columns from catalog

Read `catalog/tables/<table>.json` and present the column list:

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   BIGINT       NOT NULL
  FirstName     NVARCHAR(50) NULL
  Region        NVARCHAR(50) NULL
```

### Step 2 -- Discover writer candidates

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover refs \
  --name <table>
```

Extract the `writers` array from the output. If no writers are found, persist `no_writer_found` to catalog:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping '{"status": "no_writer_found", "selected_writer": null, "selected_writer_rationale": "No procedures found that write to this table."}'
```

Then ask the user:

> No writer found for `<table>`. Mark as a dbt source? (y/n)

If **y**, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-source \
  --name <table> --value
```

Confirm: "Marked `<table>` as a dbt source (`is_source: true`)."

If **n**, skip — the table will appear in the "pending source confirmation" section of `/status` until confirmed.

Stop here (no further steps for `no_writer_found` tables).

#### Multi-table-write disqualification

For each writer candidate, load the procedure catalog:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <candidate_procedure>
```

Check the `warnings` array for a `MULTI_TABLE_WRITE` entry. If present, **disqualify** the candidate — it writes to multiple tables and cannot produce a single clean refactored SQL. Report the disqualification to the user with the warning message but continue evaluating remaining candidates.

### Step 3 -- Analyze each writer candidate

For each **non-disqualified** writer candidate, read and follow the [procedure analysis reference](references/procedure-analysis.md). Run all 6 steps (fetch, classify, resolve call graph, logic summary, migration guidance, persist) for each candidate before moving to Step 4.

If there are multiple candidates, analyze them sequentially — each candidate's analysis must complete before starting the next.

### Step 4 -- Present writer candidates

After all candidates are analyzed, present a summary:

```text
Writer candidates for silver.DimCustomer:

  1. dbo.usp_load_dimcustomer_full (direct writer)
     Reads: bronze.Customer, bronze.Person
     Writes: silver.DimCustomer
     Statements: 1 migrate, 1 skip

  2. dbo.usp_load_dimcustomer_delta (direct writer)
     Reads: bronze.Customer, silver.DimCustomer
     Writes: silver.DimCustomer
     Statements: 1 migrate (MERGE)
```

Include rationale (direct writer, transitive writer), dependencies (reads/writes), and statement summary for each candidate.

### Step 5 -- Resolution

Apply resolution rules:

- **1 writer** -- auto-select, confirm with user before persisting
- **2+ writers** -- present candidates and ask the user to pick
- **0 writers** -- report `no_writer_found` (already handled in Step 2)

Wait for explicit user confirmation before proceeding to Step 6.

### Step 6 -- Persist scoping to catalog

Write the scoping JSON to a temp file to avoid shell quoting issues (rationale text may contain apostrophes):

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json; rm -rf .staging
```

The scoping JSON must include the selected writer (or `no_writer_found` status) and a `selected_writer_rationale` field (1–2 sentences explaining why this writer was chosen over alternatives, or why no writer / ambiguous). If the write exits non-zero, report the error and ask the user to correct.

## References

- [references/procedure-analysis.md](references/procedure-analysis.md) — six-step deep-dive pipeline: fetch, classify, call graph, logic summary, migration guidance, persist
- [references/tsql-parse-classification.md](references/tsql-parse-classification.md) — LLM fallback classification tables for migrate/skip statements, control flow, and dynamic SQL

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover refs` | 1 | Object not found or catalog file missing. Report and stop |
| `discover refs` | 2 | Catalog directory unreadable (IO error). Report and stop |
| procedure analysis | reference failure | Log failure, mark candidate `BLOCKED`, continue with remaining |
| `discover write-scoping` | 1 | Validation failure. Report errors, ask user to correct |
| `discover write-scoping` | 2 | Invalid JSON or IO error. Report and stop |
| `discover write-source` | 1 | Catalog file missing or table not analyzed. Report and stop |
