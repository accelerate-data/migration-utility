---
name: scoping-table
description: >
  Writer discovery, scope resolution, and catalog persistence for a single table. Finds which stored procedures write to the table, delegates each candidate to /analyzing-object for full procedure analysis, then lets the user confirm the selected writer and persists the scoping decision to the table catalog file.
user-invocable: true
argument-hint: "--table <fqn>"
---

# Scope Table

Discover writers for a table, analyze each candidate, resolve which writer owns the table, and persist the scoping decision to the catalog.

## Arguments

Parse `$ARGUMENTS` for `--table`. Use `AskUserQuestion` if `--table` is missing.

| Option | Required | Description |
|---|---|---|
| `--table` | yes | Fully-qualified table name (e.g. `silver.DimCustomer`, `[dbo].[FactSales]`) |

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, stop and tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, stop and tell the user to run `/listing-objects --type tables` first.

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
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover refs \
  --name <table>
```

Extract the `writers` array from the output. If no writers are found, report `no_writer_found`, persist that scoping to the catalog (Step 6), and stop.

### Step 3 -- Analyze each writer candidate

For each writer candidate, delegate to the analyzing-object skill:

```text
/analyzing-object --name <writer>
```

Do NOT inline call graph resolution, statement classification, or any procedure analysis logic here. The `/analyzing-object` skill owns that entire flow. Wait for it to complete before proceeding to the next candidate.

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

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover write-scoping \
  --name <table> --scoping '<json>'
```

The scoping JSON must include the selected writer (or `no_writer_found` status) and a `selected_writer_rationale` field (1–2 sentences explaining why this writer was chosen over alternatives, or why no writer / ambiguous). If the write exits non-zero, report the error and ask the user to correct.

## Error handling

| Condition | Action |
|---|---|
| `manifest.json` missing | Stop, tell user to run `setup-ddl` |
| `catalog/tables/<table>.json` missing | Stop, tell user to run `discover list --type tables` |
| `discover refs` exits non-zero | Report error, stop |
| `/analyzing-object` fails for a candidate | Log the failure, mark that candidate as `BLOCKED`, continue with remaining candidates |
| `discover write-scoping` exits non-zero | Report validation errors, ask user to correct |
