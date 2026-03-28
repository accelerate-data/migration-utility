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

Instructions for using `discover` to explore a DDL artifact directory.

## DDL path

Before running any subcommand, ask the user for the path to the directory
containing their `.sql` files.  Do not assume `./artifacts/ddl` or any other
default — the user chooses where their DDL lives.  The directory may contain
any number of `.sql` files with any names; object types are auto-detected
from `CREATE` statements inside.

## Invoking discover

`discover` has three subcommands: `list`, `show`, and `refs`. All subcommands require `--ddl-path`.

### list — enumerate objects in the DDL directory

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list --ddl-path ./artifacts/ddl --type tables
```

Valid values for `--type`: `tables`, `procedures`, `views`, `functions`.

Output shape:

```json
{ "objects": ["dbo.DimCustomer", "dbo.FactSales", "silver.FactReturns"] }
```

### show — inspect a single object

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show --ddl-path ./artifacts/ddl --name dbo.FactSales
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
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs --ddl-path ./artifacts/ddl --name dbo.FactSales
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

If `discover` exits with code 2, a DDL block in the directory could not be
parsed by sqlglot (e.g. procedures with `IF/ELSE`, `MERGE`, or multiple
statements).  The error message names the specific object that failed.

Tell the user which object failed and ask them to either:

1. Remove or isolate the unparseable object into a separate directory.
2. Simplify the DDL so sqlglot can parse it.

Do not silently skip unparseable objects — the loader treats parse failures
as hard errors.
