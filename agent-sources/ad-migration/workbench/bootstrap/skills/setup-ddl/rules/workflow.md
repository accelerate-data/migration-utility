# setup-ddl Workflow

End-to-end step sequence for the `setup-ddl` skill.

## Step 1 — Pre-flight

Verify prerequisites before any SQL Server connection is attempted.

1. Run `toolbox --version`. If not found, tell the user to install genai-toolbox from the releases page and stop. Do not proceed without it.
2. Confirm environment variables are set: `MSSQL_HOST`, `MSSQL_PORT`, `SA_PASSWORD`. If any are missing, list which ones and stop.
3. Check whether the output folder already contains `.sql` files. If it does, tell the user and ask for confirmation before overwriting. Do not proceed without explicit confirmation.

**Tools used:** Bash (toolbox check), native environment inspection, Read/Glob (artifact check).

## Step 2 — Select database

List user databases (`database_id > 4`) via `mssql:mssql-execute-sql`. Present the list with a `None — exit` option. If the user picks `None`, stop immediately. Otherwise run `USE [<database>]` to set context for all subsequent queries.

**Tools used:** `mssql:mssql-execute-sql` MCP tool.

## Step 3 — Select schemas

List non-system schemas that contain at least one user object, with object counts per type. Present with an `all` option. If `all` is selected, skip schema filtering in subsequent queries. Store the selection for filtering in subsequent steps.

**Tools used:** `mssql:mssql-execute-sql` MCP tool.

## Step 4 — Export procedures, views, and functions

Run three separate queries via `mssql:mssql-execute-sql` (one per object type: `P`, `V`, `FN`/`IF`/`TF`), filtered to the selected schemas. For each result set:

- Join non-null `definition` values with `\nGO\n`.
- Append a final `\nGO\n`.
- Write the output to `<output-folder>/procedures.sql`, `views.sql`, or `functions.sql` respectively using the native Write tool.
- Skip the file if the result set is empty.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 5 — Reconstruct tables

Tables have no single-statement DDL from `OBJECT_DEFINITION()`. Run the catalog query via `mssql:mssql-execute-sql` to fetch column metadata, filtered to the selected schemas. Group rows by `(schema_name, table_name)` and build `CREATE TABLE` statements applying the type-formatting rules in the skill body. Join tables with `\nGO\n` and append a final `\nGO\n`. Write to `<output-folder>/tables.sql` using the native Write tool.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 6 — Extraction preview

Run count queries and present a summary of what will be extracted. Include object counts per type and catalog signal availability (PKs, FKs, identity columns, CDC-tracked tables). The user must confirm before catalog extraction proceeds.

If the user declines, skip Steps 7-8 and proceed to Step 9 (report summary without catalog files).

**Tools used:** `mssql:mssql-execute-sql` MCP tool.

## Step 7 — Extract catalog signals

Run bulk queries for PKs, unique indexes, FKs, identity columns, CDC, change tracking, and sensitivity classifications. Group results by table. Write per-table JSON files to `<output-folder>/catalog/tables/<schema>.<table>.json`. Each file contains the signal data; the `referenced_by` section is populated in Step 8.

Change tracking and sensitivity classifications use TRY/CATCH — if the `sys.*` view does not exist, skip gracefully.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 8 — Extract references via DMF

Run server-side cursor queries to call `sys.dm_sql_referenced_entities` for all procedures, views, and functions (one cursor per object type). Each cursor returns all DMF results in a single result set.

Process the results:

1. Group by referencing object.
2. Classify referenced entities by `referenced_class_desc` into tables/views/functions/procedures.
3. Write per-proc/view/function catalog files with outbound `references`.
4. Flip references to build `referenced_by` on table catalog files (merge with signals from Step 7).

Individual DMF errors (e.g. broken object references) are caught by TRY/CATCH in the cursor — logged and skipped, not fatal.

**Known limitation:** `sys.dm_sql_referenced_entities` resolves at definition time. Dynamic SQL references (`EXEC(@sql)`, `sp_executesql`) are invisible. These procs require LLM analysis via `discover show`.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 9 — Report summary

Print a confirmation table:

```text
DDL extraction complete → <output-folder>/
Database: <database>
Schemas:  <selected-schemas>

  DDL files:
    tables.sql     : N tables
    procedures.sql : N procedures
    views.sql      : N views
    functions.sql  : N functions

  Catalog files:
    catalog/tables/     : N files
    catalog/procedures/ : N files
    catalog/views/      : N files
    catalog/functions/  : N files
```

Tell the user they can now run the `discover` skill or invoke the `scoping-agent` against the output folder. The `discover refs` command will automatically use catalog data when available.

**Next skills:** `discover` (list/inspect objects, catalog-based refs).
