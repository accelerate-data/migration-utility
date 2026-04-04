---
name: scoping-table
description: >
  Writer discovery, procedure analysis, scope resolution, and catalog persistence for a single table. Finds which stored procedures write to the table, analyzes each candidate via the procedure-analysis reference, then lets the user confirm the selected writer and persists the scoping decision to the table catalog file.
user-invocable: true
argument-hint: "<schema.table>"
---

# Scoping Table

Discover writers for a table, analyze each candidate, resolve which writer owns the table, and persist the scoping decision to the catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified table name (e.g. `silver.DimCustomer`, `[dbo].[FactSales]`). Ask the user if missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, stop and tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, tell the user to run `/listing-objects list tables` to see available tables and stop.

## Pipeline

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

Extract the `writers` array from the output. If no writers are found, persist `no_writer_found` to catalog and stop:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping '{"status": "no_writer_found", "selected_writer": null, "selected_writer_rationale": "No procedures found that write to this table."}'
```

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

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover refs` | 1 | Object not found or catalog file missing. Report and stop |
| `discover refs` | 2 | Catalog directory unreadable (IO error). Report and stop |
| procedure analysis | reference failure | Log failure, mark candidate `BLOCKED`, continue with remaining |
| `discover write-scoping` | 1 | Validation failure. Report errors, ask user to correct |
| `discover write-scoping` | 2 | Invalid JSON or IO error. Report and stop |
