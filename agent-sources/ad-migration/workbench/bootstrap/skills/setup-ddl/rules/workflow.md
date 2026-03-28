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

## Step 6 — Report summary

Print a confirmation table:

```text
DDL extraction complete → <output-folder>/
Database: <database>
Schemas:  <selected-schemas>

  tables.sql     : N tables
  procedures.sql : N procedures
  views.sql      : N views
  functions.sql  : N functions
```

Tell the user they can now run the `discover` or `scope` skills, or invoke the `scoping-agent` against the output folder.

**Next skills:** `discover` (list/inspect objects), `scope` (find writer procedures).
