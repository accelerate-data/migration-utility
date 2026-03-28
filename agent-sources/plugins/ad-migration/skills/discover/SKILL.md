---
name: discover
description: >
  This skill should be used when the user asks to "list tables", "list procedures",
  "list views", "list functions", "show me the DDL for X", "inspect object X",
  "what references Y", or wants to explore the structure of a DDL export directory.
  Use for any object inspection or reference tracing against a local DDL snapshot.
user-invocable: false
---

# Discover

Instructions for using `discover.py` to explore a DDL artifact directory.

## Invoking discover.py

`discover.py` has three subcommands: `list`, `show`, and `refs`. All subcommands require `--ddl-path`.

### list — enumerate objects in the DDL directory

```bash
python discover.py list --ddl-path ./artifacts/ddl --type tables
```

Valid values for `--type`: `tables`, `procedures`, `views`, `functions`.

Output shape:

```json
{ "objects": ["dbo.DimCustomer", "dbo.FactSales", "silver.FactReturns"] }
```

### show — inspect a single object

```bash
python discover.py show --ddl-path ./artifacts/ddl --name dbo.FactSales
```

Output shape:

```json
{
  "name": "dbo.FactSales",
  "raw_ddl": "CREATE TABLE ...",
  "columns": [
    { "name": "SalesKey", "sql_type": "BIGINT" }
  ]
}
```

### refs — find what references an object

```bash
python discover.py refs --ddl-path ./artifacts/ddl --name dbo.FactSales
```

Output shape:

```json
{
  "name": "dbo.FactSales",
  "referenced_by": ["dbo.usp_LoadFactSales", "dbo.vw_SalesSummary"]
}
```

## Presenting the table list to the user

After running `list`, present the objects as a numbered list and prompt the user to choose:

```text
Found 5 tables:
  1. dbo.DimCustomer
  2. dbo.DimProduct
  3. dbo.DimDate
  4. silver.FactSales
  5. silver.FactReturns

Which table would you like to explore?
```

Wait for the user's selection before proceeding.

## Interpreting refs output

When `refs` returns multiple entries in `referenced_by`, group them by caller type before displaying:

- **Procedures** — any FQN whose DDL begins with `CREATE PROCEDURE` or `CREATE PROC`
- **Views** — any FQN whose DDL begins with `CREATE VIEW`

List each caller with its fully-qualified name. Note that `refs` reports a reference relationship only — a caller appearing here means it mentions the target object, not necessarily that it writes to it. Use `scope.py` to determine which callers actually perform write operations.

Example display:

```text
dbo.FactSales is referenced by:

Procedures (2):
  - dbo.usp_LoadFactSales
  - dbo.usp_ArchiveFactSales

Views (1):
  - dbo.vw_SalesSummary
```

## Handling parse errors

When `show` output contains a `parse_error` field, the object could not be fully parsed by sqlglot:

1. Surface a warning to the user: "Warning: this object could not be fully parsed — DDL analysis may be incomplete."
2. Show the `raw_ddl` field so the user can inspect the source manually.
3. Do not abort the workflow — continue to the next step.

Example output with parse error:

```json
{
  "name": "dbo.usp_LegacyLoad",
  "raw_ddl": "CREATE PROCEDURE dbo.usp_LegacyLoad AS ...",
  "columns": [],
  "parse_error": "Unsupported syntax at line 42: OPTION (RECOMPILE)"
}
```

Display format:

```text
Warning: dbo.usp_LegacyLoad could not be fully parsed.
  Reason: Unsupported syntax at line 42: OPTION (RECOMPILE)

Raw DDL is available for manual inspection. Proceeding.
```
