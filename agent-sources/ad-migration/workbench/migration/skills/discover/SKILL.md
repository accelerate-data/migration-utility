---
name: discover
description: >
  This skill should be used when the user asks to "list tables", "list procedures", "list views", "list functions", "show me the DDL for X", "inspect object X", "what references Y", "find what writes to [table]", "which procedures populate [table]", or wants to explore the structure of a DDL export directory. Use for any object inspection, reference tracing, or writer discovery against a local DDL snapshot.
user-invocable: true
argument-hint: "[ddl-path] [subcommand] [options]"
---

# Discover

Explore a DDL artifact directory. Requires catalog files from `setup-ddl` — errors if catalog is missing.

## Arguments

Parse `$ARGUMENTS` for `ddl-path` and optionally a subcommand with its options. If `ddl-path` is missing, default to the current working directory. Use `AskUserQuestion` to show the user the resolved path and get confirmation before proceeding. If no subcommand is specified, default to `list`.

### Subcommands

**list** — enumerate objects by type:

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--type` | yes | `tables`, `procedures`, `views`, `functions` |

**show** — inspect a single object (columns, params, refs, raw DDL):

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name (e.g. `dbo.FactSales`, `[silver].[DimProduct]`) |

**refs** — find all procedures/views that reference an object (readers, writers from catalog):

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name |

## Before invoking any subcommand

Read `<ddl-path>/manifest.json` to confirm the directory is a valid DDL extraction and to understand the source technology and dialect. If the manifest is missing, stop and tell the user to run `setup-ddl` first.

## Output schemas

| Subcommand | Schema |
|---|---|
| `list` | `shared/shared/schemas/discover_list_output.json` |
| `show` | `shared/shared/schemas/discover_show_output.json` |
| `refs` | `shared/shared/schemas/discover_refs_output.json` |

## list

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path <ddl-path> --type <type>
```

Present as a numbered list:

```text
Found 5 tables:
  1. dbo.DimCustomer
  2. dbo.DimProduct
  3. dbo.DimDate
  4. silver.FactSales
  5. silver.FactReturns

Which object would you like to inspect?
```

If the user selects an object, proceed to `show`. If they ask what references it, proceed to `refs`.

## show

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl-path> --name <fqn>
```

Present differently based on the object type:

### Tables

Show the column list (from catalog):

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   BIGINT       NOT NULL
  FirstName     NVARCHAR(50) NULL
  Region        NVARCHAR(50) NULL
```

### Views

Show refs and the view definition:

```text
silver.vw_CustomerSales (view)

  Reads from: silver.DimCustomer, silver.FactSales

  Definition:
    SELECT c.FirstName, SUM(f.Amount) AS TotalSales
    FROM silver.DimCustomer c
    JOIN silver.FactSales f ON c.CustomerKey = f.CustomerKey
    GROUP BY c.FirstName
```

### Procedures

Always read `raw_ddl` to understand the procedure body. Use `refs` and `statements` (when available) to supplement, but the body is the source of truth.

Check `classification` to decide how much help you get:

1. `deterministic` with `statements` populated — `refs` and `statements` are pre-classified, use them alongside the body as the authoritative source of truth.
2. `claude_assisted` or `statements` is null — classify each statement yourself from the body. See [`references/tsql-parse-classification.md`](references/tsql-parse-classification.md) for classification guidance.

Present three sections: **Call Graph**, **Logic Summary** and **Migration Guidance**.

**Call Graph** — read/write targets from `refs`. Resolve to base tables: if a ref is a view, function, or procedure, run `discover show` on it to get its refs, and follow the chain until you reach base tables. Present the full lineage in the call graph.

**Logic Summary** — always produced by reading `raw_ddl`. Plain-language description of what the procedure does, step by step. No tags, no classification — just explain the logic.

**Migration Guidance** — tag each statement as `migrate` or `skip`:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) — becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) — dbt handles or ignores |

```text
Call Graph
  silver.usp_load_DimCustomer  (direct writer)
    ├── reads: silver.vw_ProductCatalog (view)
    │     ├── reads: bronze.Customer        ← resolved via discover show
    │     └── reads: bronze.Product         ← resolved via discover show
    ├── reads: bronze.Person
    └── writes: silver.DimCustomer

Logic Summary
  This procedure performs a full reload of silver.DimCustomer. It reads
  from vw_ProductCatalog (which joins bronze.Customer and bronze.Product),
  joins with bronze.Person, computes DateFirstPurchase via OUTER APPLY on
  bronze.SalesOrderHeader, and inserts into silver.DimCustomer.

Migration Guidance
  1. [skip]    TRUNCATE TABLE silver.DimCustomer
  2. [migrate] INSERT INTO silver.DimCustomer from vw_ProductCatalog JOIN bronze.Person
  3. [migrate] Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

### Persisting Resolved Statements

After reviewing statements with the user, persist resolved statements to catalog:

**For deterministic procedures** (`classification: deterministic`, no `claude` actions in statements):

Run directly:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover write-statements \
  --ddl-path <ddl_path> --name <procedure_name> --statements '<json>'
```

All statements get `source: "ast"`.

**For claude-assisted procedures** (`classification: claude_assisted` or statements containing `action: "claude"`):

1. Read `raw_ddl` and analyse each `claude` statement — follow the call graph, resolve dynamic SQL, and classify as `migrate` or `skip`.
2. Present the full resolved statement list to the FDE for confirmation. Show each statement with its proposed action and rationale.
3. After FDE confirms (with any edits), run `discover write-statements` to persist. All resolved statements get `source: "llm"`.

No `claude` actions are written to catalog — all must be resolved before persisting.

## refs

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl-path> --name <fqn>
```

The output contains `writers` (procs that modify the table) and `readers` (procs/views that select from it).

Present grouped:

```text
silver.FactSales references:

  Writers (1):
    - dbo.usp_load_FactSales

  Readers (2):
    - dbo.usp_read_fact_sales
    - dbo.vw_sales_summary
```

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers. Use `discover show` on suspected procs to confirm via `raw_ddl` analysis.

## Parse errors

Procedures with `parse_error` set are still loaded — not skipped. Their `raw_ddl` is preserved for inspection. The `parse_error` field explains why sqlglot could not fully parse the body.

If `discover` exits with code 2, the directory itself could not be read (missing path, IO error, no catalog). Individual proc parse failures do not cause exit code 2.
