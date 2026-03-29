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

Parse `$ARGUMENTS` for `ddl-path` and optionally a subcommand with its options. If `ddl-path` is missing, ask the user for it. Do not assume any default path. If no subcommand is specified, default to `list`.

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

Always present a call graph and logic summary.

Check `classification` first, then fall back to `statements`:

1. If `classification` is `deterministic` and `statements` is populated — use `refs` and `statements` directly
2. If `classification` is `claude_assisted` or `statements` is null — read `raw_ddl` and analyse the proc body yourself

Both paths produce the same output for the user:

```text
Call Graph

  silver.usp_load_DimCustomer  (direct writer)
    ├── reads: bronze.Customer
    ├── reads: bronze.Person
    └── writes: silver.DimCustomer

Logic Summary
  1. TRUNCATE TABLE silver.DimCustomer
  2. INSERT INTO silver.DimCustomer from JOIN of bronze.Customer and bronze.Person
  3. Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

#### Statement actions

Each statement in a proc body is either:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) — becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) — dbt handles or ignores |

For deterministic procs, the `statements` array has these pre-classified. For claude_assisted procs, read `raw_ddl` and classify each statement yourself.

See [`references/tsql-parse-classification.md`](references/tsql-parse-classification.md) for the exhaustive pattern list.

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
