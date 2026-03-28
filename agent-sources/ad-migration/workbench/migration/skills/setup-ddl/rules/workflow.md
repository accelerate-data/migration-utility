# setup-ddl Workflow

End-to-end step sequence for the `setup-ddl` skill.

## Step 1 — Pre-flight

Verify prerequisites before any SQL Server connection is attempted.

1. Run `toolbox --version`. If not found, tell the user to install genai-toolbox from the releases page and stop. Do not proceed without it.
2. Confirm all four environment variables are set: `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. If any are missing, list which ones and stop.
3. Check whether `artifacts/ddl/` already contains `.sql` files. If it does, tell the user and ask for confirmation before overwriting. Do not proceed without explicit confirmation.

**Tools used:** Bash (toolbox check), native environment inspection, Read/Glob
(artifact check).

## Step 2 — Export procedures, views, and functions

Run three separate queries via `mssql:mssql-execute-sql` (one per object type: `P`, `V`, `FN`/`IF`/`TF`). For each result set:

- Join non-null `definition` values with `\nGO\n`.
- Append a final `\nGO\n`.
- Write the output to `artifacts/ddl/procedures.sql`, `views.sql`, or `functions.sql` respectively using the native Write tool.
- Write an empty file if the result set is empty — the `ddl` MCP server expects all four files to exist.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 3 — Reconstruct tables

Tables have no single-statement DDL from `OBJECT_DEFINITION()`. Run the catalog query via `mssql:mssql-execute-sql` to fetch column metadata. Group rows by `(schema_name, table_name)` and build `CREATE TABLE` statements applying the type-formatting rules in the skill body. Join tables with `\nGO\n` and append a final `\nGO\n`. Write to `artifacts/ddl/tables.sql` using the native Write tool.

**Tools used:** `mssql:mssql-execute-sql` MCP tool, native Write tool.

## Step 4 — Write files

Confirm all four files exist in `artifacts/ddl/`:

- `tables.sql`
- `procedures.sql`
- `views.sql`
- `functions.sql`

If any file is missing (for example, due to an empty result set that was not written), create it as an empty file. The `ddl` MCP server requires all four to be present.

**Tools used:** Glob (existence check), native Write tool (if missing).

## Step 5 — Report summary

Print a confirmation table:

```text
DDL extraction complete → artifacts/ddl/

  tables.sql     : N tables
  procedures.sql : N procedures
  views.sql      : N views
  functions.sql  : N functions
```

Tell the user they can now run the `discover` or `scope` skills, or invoke the `scoping-agent` against `artifacts/ddl/`.

**Next skills:** `discover` (list/inspect objects), `scope` (find writer procedures).
