# Stage 2 -- DDL Extraction

The `/setup-ddl` skill connects to a live SQL Server via MCP, extracts DDL for selected schemas, builds catalog files enriched with 12 signal queries, and runs AST enrichment. All downstream pipeline stages depend on the catalog this produces.

## Prerequisites

- `toolbox` binary on PATH (`toolbox --version`)
- All four MSSQL environment variables set: `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`
- These must be set before launching `claude` -- the MCP server reads them at startup

## Two Modes

| Mode | Requirements | How it works |
|---|---|---|
| Live DB (default) | `toolbox` + MSSQL env vars | Connects to SQL Server via MCP, runs all queries live |
| DDL file import | None | Uses `listing-objects`, `analyzing-table`, and `scoping` skills against local DDL files |

This page covers the live DB mode. DDL file mode is available when `toolbox` or MSSQL credentials are not configured.

## Interactive Walkthrough

The skill walks through three interactive steps before writing any files.

### Step 1 -- Database selection

Lists all user databases on the server and asks you to pick one:

```sql
SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name
```

### Step 2 -- Schema selection

Lists non-system schemas with object counts:

```text
schema_name  | tables | procedures | views | functions
-------------|--------|------------|-------|----------
dbo          |     12 |          8 |     3 |         2
silver       |     25 |         15 |     5 |         0
```

You can select individual schemas or `all`. The selected schemas filter all subsequent extraction queries.

### Step 3 -- Extraction preview and confirmation

Before any files are written, the skill presents a summary:

```text
Extraction preview for [AdventureWorksDW]
Schemas: dbo, silver

  Object counts:
    Tables:     37
    Procedures: 23
    Views:       8
    Functions:   2

  Catalog signals available:
    Primary keys:    28 constraints
    Foreign keys:    15 constraints
    Identity cols:   12 columns
    CDC-tracked:      3 tables

  DDL files will be written to:     ./ddl/
  Catalog files will be written to: ./catalog/
```

You must confirm before extraction proceeds. If you decline, no files are written.

## What It Produces

### manifest.json

Written first, captures the source database, technology, and selected schemas:

```bash
uv run --project <shared-path> setup-ddl write-manifest \
  --technology sql_server \
  --database <database> \
  --schemas <comma-separated-schemas>
```

### DDL files (`ddl/`)

| File | Contents |
|---|---|
| `tables.sql` | CREATE TABLE statements with column definitions, identity columns |
| `procedures.sql` | Full procedure definitions |
| `views.sql` | View definitions |
| `functions.sql` | Scalar, inline, and table-valued function definitions |

### Catalog files (`catalog/`)

One JSON file per object, organized by type:

```text
catalog/
  tables/
    silver.DimCustomer.json
    silver.FactInternetSales.json
  procedures/
    dbo.usp_load_dimcustomer.json
  views/
    silver.vw_sales_summary.json
  functions/
    dbo.fn_calculate_tax.json
```

## Catalog Signals

The extraction runs 12 signal queries against `sys.*` views and `sys.dm_sql_referenced_entities` to populate catalog files:

| Signal | Source | What it captures |
|---|---|---|
| Primary keys | `sys.indexes` + `sys.index_columns` | PK and unique constraints with column ordinals |
| Foreign keys | `sys.foreign_keys` + `sys.foreign_key_columns` | FK relationships with referenced table and column |
| Identity columns | `sys.identity_columns` | Auto-increment columns with seed and increment |
| CDC tracking | `sys.tables.is_tracked_by_cdc` | Tables with change data capture enabled |
| Change tracking | `sys.change_tracking_tables` | Tables with change tracking enabled (graceful -- skips if unavailable) |
| Sensitivity classifications | `sys.sensitivity_classifications` | Data classification labels and information types (graceful -- skips if unavailable) |
| Object type map | `sys.objects` | Maps every object to its type (table, view, proc, function) |
| Definitions | `OBJECT_DEFINITION()` | Full source code for procs, views, and functions |
| Procedure parameters | `sys.parameters` | Parameter names, types, and output flags |
| Procedure DMF refs | `sys.dm_sql_referenced_entities` | Column-level `is_selected`/`is_updated` flags for procedure references |
| View DMF refs | `sys.dm_sql_referenced_entities` | Same for views |
| Function DMF refs | `sys.dm_sql_referenced_entities` | Same for functions |

## AST Enrichment

After catalog files are written, an AST enrichment pass fills gaps that `sys.dm_sql_referenced_entities` cannot detect:

```bash
uv run --project <shared-path> catalog-enrich --project-root .
```

This augments catalog files with AST-derived references for:

- CTAS / `SELECT INTO` targets (catalog queries miss new table creation)
- `TRUNCATE` targets
- Indirect writers through `EXEC` call chains

Entries added carry `"detection": "ast_scan"` to distinguish from catalog-query-sourced data.

### Known limitation

Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. This is an inherent offline limitation of `sys.dm_sql_referenced_entities`, which resolves references at definition time, not runtime. These procs require LLM analysis during the scoping stage.

## Re-running

If `manifest.json` already exists, the skill reads `source_database` and `extracted_schemas` from it and skips database selection. If `ddl/` or `catalog/` directories exist, the skill warns that re-running will fully rebuild both directories and asks for confirmation.

## Final Report

```text
DDL extraction complete
Database: AdventureWorksDW
Schemas:  dbo, silver

  DDL files (ddl/):
    tables.sql     : 37 tables
    procedures.sql : 23 procedures
    views.sql      :  8 views
    functions.sql  :  2 functions

  Catalog files (catalog/):
    tables/     : 37 files
    procedures/ : 23 files
    views/      :  8 files
    functions/  :  2 files

  manifest.json at ./manifest.json
```

## Oracle Extraction

Use the `extract` subcommand for Oracle sources instead of the interactive `/setup-ddl` skill:

```bash
uv run --project <shared-path> setup-ddl extract \
  --schemas SH,HR \
  [--project-root .]
```

### Oracle prerequisites

The Oracle user must have read access to the `ALL_*` catalog views and `DBMS_METADATA` before running extraction:

```sql
GRANT SELECT_CATALOG_ROLE TO <user>;
GRANT SELECT ANY DICTIONARY TO <user>;
```

Without `SELECT_CATALOG_ROLE`, `DBMS_METADATA.GET_DDL` fails for objects owned by other schemas.

### Oracle catalog signals

| Signal | Source | Notes |
|---|---|---|
| Table columns | `ALL_TAB_COLUMNS` | Maps Oracle types to SQL Server-compatible field names |
| Primary keys and unique indexes | `ALL_CONSTRAINTS` + `ALL_CONS_COLUMNS` (`TYPE IN ('P','U')`) | |
| Foreign keys | `ALL_CONSTRAINTS` + `ALL_CONS_COLUMNS` (`TYPE='R'`) | Joins through referenced constraint to resolve column names |
| Identity columns | `ALL_TAB_COLUMNS WHERE IDENTITY_COLUMN='YES'` | Oracle 12c+ only; trigger-based sequences are not detected |
| Object type map | `ALL_OBJECTS` | Maps Oracle types to SQL Server codes: TABLE→U, VIEW→V, PROCEDURE→P, FUNCTION→FN |
| Procedure/view/function definitions | `DBMS_METADATA.GET_DDL` per object | Per-object call with try/except; skips objects where DDL retrieval fails |
| Dependency references | `ALL_DEPENDENCIES` | Object-level only — all boolean flags (`is_selected`, `is_updated`, etc.) are always `False` |
| Procedure parameters | `ALL_ARGUMENTS WHERE PACKAGE_NAME IS NULL` | Standalone procedures and functions only |

### Oracle limitations

| Feature | Status |
|---|---|
| CDC tracking | Not supported — `cdc.json` is always empty |
| Change tracking | Not supported — `change_tracking.json` is always empty |
| Sensitivity classifications | Not supported — `sensitivity.json` is always empty |
| Column-level DMF flags | Always `False` — `ALL_DEPENDENCIES` has no column-level detail |
| Auto-increment mechanism | `IDENTITY` columns only — trigger/sequence patterns not detected |
| Package procedures | Not extracted — `ALL_ARGUMENTS` filtered to `PACKAGE_NAME IS NULL` |

## Next Step

Proceed to [[Stage 3 dbt Scaffolding]] to scaffold your dbt project from the extracted catalog.
