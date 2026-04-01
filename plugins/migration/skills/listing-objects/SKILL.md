---
name: listing-objects
description: >
  Use when the user asks to "list tables", "list procedures", "list views", "list functions", "show me object X", "what references Y", or wants to browse catalog contents. This is a read-only viewer — it displays whatever state the catalog has but never writes to it.
user-invocable: true
argument-hint: "[subcommand] [options]"
---

# Listing Objects

Read-only catalog viewer. Shows whatever state exists in the catalog at any point in the pipeline — columns, refs, scoping candidates, analyzed statements. Never writes to the catalog.

## Arguments

Parse `$ARGUMENTS` for a subcommand and its options. If no subcommand is specified, default to `list`.

### Subcommands

**list** — enumerate objects by type:

| Option | Required | Values |
|---|---|---|
| `--type` | yes | `tables`, `procedures`, `views`, `functions` |

**show** — display current catalog state for a single object:

| Option | Required | Values |
|---|---|---|
| `--name` | yes | fully-qualified object name (e.g. `dbo.FactSales`, `[silver].[DimProduct]`) |

**refs** — find all procedures/views that reference an object (readers, writers from catalog):

| Option | Required | Values |
|---|---|---|
| `--name` | yes | fully-qualified object name |

## Before invoking any subcommand

Read `manifest.json` from the current working directory to confirm it is a valid project root and to understand the source technology and dialect. If the manifest is missing, stop and tell the user to run `setup-ddl` first.

## Output schemas

| Subcommand | Schema |
|---|---|
| `list` | `lib/shared/schemas/discover_list_output.json` |
| `show` | `lib/shared/schemas/discover_show_output.json` |
| `refs` | `lib/shared/schemas/discover_refs_output.json` |

## list

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover list \
  --type <type>
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
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <fqn>
```

Present whatever the catalog currently holds for the object. This is strictly read-only — do not trigger analysis, scoping, or any catalog writes.

**Tables:** show columns. If scoping candidates exist in the catalog, display them. If analyzed statements exist for associated procedures, display those too.

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   BIGINT       NOT NULL
  FirstName     NVARCHAR(50) NULL
  Region        NVARCHAR(50) NULL

  Scoping: dbo.usp_load_dimcustomer_full (selected writer)
  Statements: 1 migrate, 1 skip
```

If no scoping or statements exist yet, just show the columns and say so.

**Views:** show refs and the view definition.

```text
silver.vw_CustomerSales (view)

  Reads from: silver.DimCustomer, silver.FactSales

  Definition:
    SELECT c.FirstName, SUM(f.Amount) AS TotalSales
    FROM silver.DimCustomer c
    JOIN silver.FactSales f ON c.CustomerKey = f.CustomerKey
    GROUP BY c.FirstName
```

**Procedures:** show whatever catalog state exists — parameters, refs, statements if analyzed, raw DDL summary. Do not run the analysis flow.

## refs

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover refs \
  --name <fqn>
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

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers. Use `show` on suspected procs to confirm via `raw_ddl` inspection.

## Parse errors

Procedures with `parse_error` set are still loaded — not skipped. Their `raw_ddl` is preserved for inspection. The `parse_error` field explains why sqlglot could not fully parse the body.

If `discover` exits with code 2, the directory itself could not be read (missing path, IO error, no catalog). Individual proc parse failures do not cause exit code 2.
