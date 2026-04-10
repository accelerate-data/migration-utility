---
name: analyzing-table
description: >
  Analyzes a single table, view, or materialized view for migration scoping. For tables: discovers writer procedures, analyzes candidates, resolves the selected writer. For views/MVs: extracts SQL elements, builds call tree, generates logic summary. Auto-detects object type from catalog presence.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Analyzing Table

Analyze a table, view, or materialized view — discover writer candidates, evaluate them, and persist the scoping decision to the catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified name (e.g. `silver.DimCustomer`, `silver.vw_CustomerSales`). Ask the user if missing.

## Schema discipline

Whenever this skill writes structured JSON back to the catalog, treat the schemas in `../../lib/shared/schemas/` as the contract:

- table scoping: `table_catalog.json#/properties/scoping`
- view scoping: `view_catalog.json#/properties/scoping`
- procedure statements: `procedure_catalog.json#/properties/statements`

Do not invent field names or omit required fields. The examples in this skill are minimum valid shapes, not loose suggestions. If `discover write-scoping` or `discover write-statements` returns a schema validation error, fix the JSON to match the schema and retry the command.

Use the canonical `/scope` surfaced code list in `../../lib/shared/scope_error_codes.md`. Do not define a competing public error-code list in this skill.

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> scope
```

If `ready` is `false`, report the failing check's `code` and `reason` to the user and stop.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → this is a **view or MV**. Follow the **View Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Pipeline** below.

---

## View Pipeline

### Step V1 -- Show view from catalog

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

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

### Step V5 -- Persist scoping to catalog

Persist the view analysis as soon as the scoping JSON is ready. Do not ask for confirmation before writing — this skill is a write-through workflow.

Write the scoping JSON to a temp file:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <view_fqn> --scoping-file .staging/scoping.json && rm -rf .staging
```

Do not include `status` in the scoping dict.

The scoping JSON shape:

```json
{
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

### Step V6 -- Present persisted result

After `discover write-scoping` succeeds, present the persisted result to the user:

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

Persisted to catalog/views/silver.vw_customersales.json.
```

If the view depends on other views, show the warning prominently:

```text
⚠ VIEW_DEPENDS_ON_VIEWS: silver.vw_addressbase has not been analyzed yet.
  Run /scope silver.vw_addressbase first for accurate profiling results.
```

---

## Table Pipeline

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
  --name <table> --scoping '{"selected_writer": null, "selected_writer_rationale": "No procedures found that write to this table."}'
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

#### Multi-table-writer handling

If a candidate proc has a `MULTI_TABLE_WRITE` warning, do **not** disqualify it. Instead, assess whether the logic is separable or truly interleaved:

**Truly interleaved** — a single MERGE/INSERT block writes to both tables simultaneously, or the logic uses shared variables/transaction semantics that cannot be cleanly attributed to one table:

- Write scoping with `status: "error"` and a clear explanation of why the logic cannot be separated.
- Stop evaluating this candidate.

**Separable** — distinct MERGE/INSERT/UPDATE blocks handle each target table (shared upstream CTEs or temp table declarations are fine):

1. Identify the DDL block(s) that write to **this target table only**, including any shared setup (CTEs, temp table declarations) that those blocks depend on.
2. Write the slice to the proc catalog:

   ```bash
   mkdir -p .staging
   # Write the slice DDL to .staging/slice.sql, then:
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-slice \
     --proc <proc_fqn> --table <target_table_fqn> --slice-file .staging/slice.sql
   rm -rf .staging
   ```

3. Proceed to evaluate this candidate normally — select it with `status: "resolved"` if it is the best writer.
4. In `selected_writer_rationale`, note that this is a multi-table-writer proc and name the other tables it writes to.

### Step 3 -- Analyze each writer candidate

For each writer candidate, read and follow the [procedure analysis reference](references/procedure-analysis.md). Run all 6 steps (fetch, classify, resolve call graph, logic summary, migration guidance, persist) for each candidate before moving to Step 4.

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

- **Unsupported external delegate** -- if a candidate procedure only delegates through cross-database or linked-server `EXEC`, mark that candidate as unsupported for writer selection. Do not treat it as a valid resolved writer.
- **1 writer** -- auto-select and persist
- **2+ writers** -- present candidates, choose the best-supported writer, and persist with clear rationale
- **0 writers** -- report `no_writer_found` (already handled in Step 2)

If all discovered candidates are unsupported external delegates, persist table scoping with `status: "error"`. In that case:

- omit `selected_writer`
- explain in `selected_writer_rationale` that the apparent writer delegates to an out-of-scope external procedure and cannot be migrated from this project
- include an `errors` entry with code `REMOTE_EXEC_UNSUPPORTED`

### Step 6 -- Persist scoping to catalog

Write the scoping JSON to a temp file:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json && rm -rf .staging
```

Do not include `status` in the scoping dict.

The scoping JSON must include the selected writer and a `selected_writer_rationale` field (1–2 sentences explaining why this writer was chosen over alternatives, or why no writer / ambiguous). If the write exits non-zero, report the error and ask the user to correct.

The table scoping JSON shape:

```json
{
  "selected_writer": "silver.usp_load_dimcustomer_full",
  "selected_writer_rationale": "Full loader is the primary writer because it independently rebuilds the target table from source data.",
  "candidates": [
    {
      "procedure_name": "silver.usp_load_dimcustomer_full",
      "rationale": "Direct full-load writer for the target table.",
      "dependencies": {
        "tables": ["bronze.customer", "bronze.person"],
        "views": [],
        "functions": []
      }
    },
    {
      "procedure_name": "silver.usp_load_dimcustomer_delta",
      "rationale": "Incremental MERGE writer; supplementary rather than primary."
    }
  ],
  "warnings": [],
  "errors": []
}
```

For multi-writer cases, every entry in `candidates` must use `procedure_name` and `rationale`. `dependencies` is optional. Do not use legacy fields such as `procedure`, `write_type`, or `selected`.

For unsupported external delegate cases, the scoping JSON should look like:

```json
{
  "selected_writer_rationale": "The only discovered writer delegates through a linked-server or cross-database EXEC, which is out of scope for this migration project.",
  "candidates": [
    {
      "procedure_name": "silver.usp_scope_linkedserverexec",
      "rationale": "Delegates to an external procedure through EXEC and is not a migratable writer candidate."
    }
  ],
  "warnings": [],
  "errors": [
    {
      "code": "REMOTE_EXEC_UNSUPPORTED",
      "message": "Writer delegates through linked-server or cross-database EXEC, which is out of scope.",
      "severity": "error"
    }
  ]
}
```

After `discover write-scoping` succeeds, present the persisted result to the user.

## References

- [references/procedure-analysis.md](references/procedure-analysis.md) — six-step deep-dive pipeline: fetch, classify, call graph, logic summary, migration guidance, persist
- [references/tsql-parse-classification.md](references/tsql-parse-classification.md) — LLM fallback classification tables for migrate/skip statements, control flow, and dynamic SQL
- [`../../lib/shared/scope_error_codes.md`](../../lib/shared/scope_error_codes.md) — canonical `/scope` statuses and surfaced error/warning codes

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover refs` | 1 | Object not found or catalog file missing. Report and stop |
| `discover refs` | 2 | Catalog directory unreadable (IO error). Report and stop |
| procedure analysis | reference failure | Log failure, mark candidate `BLOCKED`, continue with remaining |
| `discover write-scoping` | 1 | Validation failure. Report errors, ask user to correct |
| `discover write-scoping` | 2 | Invalid JSON or IO error. Report and stop |
| `discover write-source` | 1 | Catalog file missing or table not analyzed. Report and stop |
