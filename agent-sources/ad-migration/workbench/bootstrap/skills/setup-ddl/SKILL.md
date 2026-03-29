---
name: setup-ddl
description: >
  This skill should be used when the user asks to "set up DDL", "extract DDL from SQL Server", "populate artifacts/ddl", "connect to the remote database and get DDL", "pull DDL from the source database", or wants to initialise the local DDL artifact directory from a live SQL Server before running discovery or scoping.
argument-hint: "[output-folder]"
---

# Setup DDL

Extract DDL from a live SQL Server and write local artifact files that the `ddl` MCP server and `discover`/`scope` tools can read.

## Arguments

Parse `$ARGUMENTS`:

- `output-folder` (required): path where `.sql` files will be written (e.g. `./artifacts/ddl`)

If `output-folder` is missing from `$ARGUMENTS`, stop immediately and tell the user to provide it. Do not assume any default path.

## Prerequisites

Before starting, verify:

1. **`toolbox` binary is on PATH** — run `toolbox --version`. If not found, direct the user to install genai-toolbox from `https://github.com/googleapis/genai-toolbox/releases` and add it to PATH. Stop here if not installed.

2. **Environment variables are set** — the `mssql` MCP server reads these at startup:

   | Variable | Description | Example |
   |---|---|---|
   | `MSSQL_HOST` | SQL Server hostname or IP | `localhost` |
   | `MSSQL_PORT` | SQL Server port | `1433` |
   | `SA_PASSWORD` | SQL login password | _(from env)_ |

   `MSSQL_DB` is not required at this stage — the skill selects the database interactively. Confirm `MSSQL_HOST`, `MSSQL_PORT`, and `SA_PASSWORD` are set. If any are missing, tell the user and stop.

3. **Pre-flight check** — check whether the output folder already exists and contains `.sql` files. If it does, tell the user and ask for confirmation before overwriting. Do not proceed without explicit confirmation.

## Workflow

Follow the step sequence in [`rules/workflow.md`](rules/workflow.md) for the full pre-flight → select database → select schemas → export → report flow.

## Step 1 — Select database

List user databases on the server:

```sql
SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name
```

Present the list with a `0. None — exit` option. If the user picks `None`, stop immediately with no further action. Once a database is selected, run `USE [<database>]` before all subsequent queries to set the database context.

## Step 2 — Select schemas

List non-system schemas with object counts so the user can see what each schema contains:

```sql
SELECT
    s.name AS schema_name,
    SUM(CASE WHEN o.type = 'U'  THEN 1 ELSE 0 END) AS tables,
    SUM(CASE WHEN o.type = 'P'  THEN 1 ELSE 0 END) AS procedures,
    SUM(CASE WHEN o.type = 'V'  THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN o.type IN ('FN', 'IF', 'TF') THEN 1 ELSE 0 END) AS functions
FROM sys.schemas s
JOIN sys.objects o ON o.schema_id = s.schema_id AND o.is_ms_shipped = 0
WHERE s.schema_id NOT IN (2, 3, 4)
  AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
GROUP BY s.name
ORDER BY s.name
```

Present the results with an `all` option to select every schema. Ask the user to pick `all`, one, or more schemas. If `all` is selected, do not add a schema filter to subsequent queries. Store the selected schemas for filtering in subsequent steps.

## Step 3 — Export procedures, views, and functions

Run the following query via `mssql:mssql-execute-sql` for each object type, filtered to the selected schemas.

**Procedures:**

```sql
SELECT
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS object_name,
    OBJECT_DEFINITION(o.object_id) AS definition
FROM sys.objects o
WHERE o.type = 'P'
  AND o.is_ms_shipped = 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, object_name
```

**Views:**

```sql
SELECT
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS object_name,
    OBJECT_DEFINITION(o.object_id) AS definition
FROM sys.objects o
WHERE o.type = 'V'
  AND o.is_ms_shipped = 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, object_name
```

**Functions (scalar, inline table-valued, multi-statement table-valued):**

```sql
SELECT
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS object_name,
    OBJECT_DEFINITION(o.object_id) AS definition
FROM sys.objects o
WHERE o.type IN ('FN', 'IF', 'TF')
  AND o.is_ms_shipped = 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, object_name
```

For each result set, assemble a GO-delimited file: join each non-null `definition` with `\nGO\n` and append a final `\nGO\n`. Write using the native Write tool:

- Procedures → `<output-folder>/procedures.sql`
- Views → `<output-folder>/views.sql`
- Functions → `<output-folder>/functions.sql`

If a result set is empty, skip the file (the loader auto-detects object types from whatever .sql files are present).

## Step 4 — Export tables

Tables have no single-statement DDL from `OBJECT_DEFINITION()` — reconstruct `CREATE TABLE` statements from the system catalog.

Run via `mssql:mssql-execute-sql`:

```sql
SELECT
    SCHEMA_NAME(t.schema_id)  AS schema_name,
    t.name                    AS table_name,
    c.name                    AS column_name,
    c.column_id,
    tp.name                   AS type_name,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    ic.seed_value,
    ic.increment_value
FROM sys.tables t
JOIN sys.columns c
    ON c.object_id = t.object_id
JOIN sys.types tp
    ON tp.user_type_id = c.user_type_id
LEFT JOIN sys.identity_columns ic
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
WHERE t.is_ms_shipped = 0
  AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, table_name, c.column_id
```

Group rows by `(schema_name, table_name)`. For each table, build a `CREATE TABLE [schema].[table] (...)` statement using these type formatting rules:

| Type | Format |
|---|---|
| NVARCHAR, VARCHAR, NCHAR, CHAR | `TYPE(MAX)` if max_length = -1; else `TYPE(length)` where length = max_length / 2 for N-types |
| BINARY, VARBINARY | `TYPE(MAX)` if max_length = -1; else `TYPE(max_length)` |
| DECIMAL, NUMERIC | `TYPE(precision, scale)` |
| FLOAT, REAL | `TYPE` (no size) |
| All others | `TYPE` (no size) |

For each column:

- Add `IDENTITY(seed, increment)` if `is_identity = 1`
- Add `NOT NULL` if `is_nullable = 0`, else `NULL`

Assemble columns separated by `,\n`. Wrap in `CREATE TABLE [schema].[table] (\n...\n)`.
Join all tables with `\nGO\n` and append a final `\nGO\n`.

Write to `<output-folder>/tables.sql` using the native Write tool.

## Step 5 — Extraction preview

Before writing any files, run count queries and present a summary so the user knows what will be extracted:

```sql
SELECT
    SUM(CASE WHEN o.type = 'U'  THEN 1 ELSE 0 END) AS tables,
    SUM(CASE WHEN o.type = 'P'  THEN 1 ELSE 0 END) AS procedures,
    SUM(CASE WHEN o.type = 'V'  THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN o.type IN ('FN', 'IF', 'TF') THEN 1 ELSE 0 END) AS functions
FROM sys.objects o
WHERE o.is_ms_shipped = 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
```

Also check catalog signal availability:

```sql
SELECT
    (SELECT COUNT(*) FROM sys.key_constraints WHERE type = 'PK') AS pk_count,
    (SELECT COUNT(*) FROM sys.foreign_keys) AS fk_count,
    (SELECT COUNT(*) FROM sys.identity_columns) AS identity_count,
    (SELECT COUNT(*) FROM sys.tables WHERE is_tracked_by_cdc = 1) AS cdc_count
```

Present as a clear summary table:

```text
Extraction preview for [database]
Schemas: <selected-schemas>

  Object counts:
    Tables:     N
    Procedures: N
    Views:      N
    Functions:  N

  Catalog signals available:
    Primary keys:    N constraints
    Foreign keys:    N constraints
    Identity cols:   N columns
    CDC-tracked:     N tables

  Catalog files will be written to: <output-folder>/catalog/
  Reference data from sys.dm_sql_referenced_entities will be extracted
  for all procedures, views, and functions.
```

The user must confirm before extraction proceeds. If they decline, stop immediately — no files are written.

## Step 6 — Extract catalog signals

After user confirmation, extract catalog signals for all tables in the selected schemas. These are written as per-table JSON files under `<output-folder>/catalog/tables/`.

Run the following queries via `mssql:mssql-execute-sql`:

**Primary keys and unique indexes:**

```sql
SELECT
    SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
    i.name AS index_name, i.is_unique, i.is_primary_key,
    c.name AS column_name, ic.key_ordinal
FROM sys.tables t
JOIN sys.indexes i ON i.object_id = t.object_id AND (i.is_primary_key = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0))
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, table_name, i.index_id, ic.key_ordinal
```

**Foreign keys:**

```sql
SELECT
    SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
    fk.name AS constraint_name,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
    SCHEMA_NAME(rt.schema_id) AS ref_schema, rt.name AS ref_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
FROM sys.foreign_keys fk
JOIN sys.tables t ON t.object_id = fk.parent_object_id
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, table_name, fk.name, fkc.constraint_column_id
```

**Identity columns:**

```sql
SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, c.name AS column_name
FROM sys.identity_columns c
JOIN sys.tables t ON t.object_id = c.object_id
WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
```

**CDC:**

```sql
SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
FROM sys.tables t
WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1
  AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
```

**Change tracking** (graceful — may not exist):

```sql
BEGIN TRY
    SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
    FROM sys.change_tracking_tables ct
    JOIN sys.tables t ON t.object_id = ct.object_id
    WHERE SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
END TRY
BEGIN CATCH
END CATCH
```

**Sensitivity classifications** (graceful — requires SQL Server 2019+):

```sql
BEGIN TRY
    SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
           sc.label, sc.information_type, COL_NAME(sc.major_id, sc.minor_id) AS column_name
    FROM sys.sensitivity_classifications sc
    JOIN sys.tables t ON t.object_id = sc.major_id
    WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
END TRY
BEGIN CATCH
END CATCH
```

For each table, assemble a JSON file at `<output-folder>/catalog/tables/<schema>.<table>.json` containing:

```json
{
  "primary_keys": [{"constraint_name": "PK_...", "columns": ["col1", "col2"]}],
  "unique_indexes": [{"index_name": "IX_...", "columns": ["col1"]}],
  "foreign_keys": [{"constraint_name": "FK_...", "columns": ["col"], "referenced_schema": "dbo", "referenced_table": "T2", "referenced_columns": ["col"]}],
  "identity_columns": ["col1"],
  "cdc_enabled": false,
  "change_tracking_enabled": null,
  "sensitivity_classifications": []
}
```

The `referenced_by` section is populated later in Step 7 after DMF extraction.

## Step 7 — Extract references via DMF

Call `sys.dm_sql_referenced_entities` for all procedures, views, and functions. Use a server-side cursor to batch all calls into one result set per object type. This avoids hundreds of individual MCP calls.

Run the following for **procedures** (repeat the same pattern for views and functions, changing the type filter):

```sql
DECLARE @result TABLE (
    referencing_schema NVARCHAR(128), referencing_name NVARCHAR(128),
    referenced_schema NVARCHAR(128), referenced_entity NVARCHAR(128),
    referenced_minor_name NVARCHAR(128), referenced_class_desc NVARCHAR(60),
    is_selected BIT, is_updated BIT, is_select_all BIT,
    is_insert_all BIT, is_all_columns_found BIT,
    is_caller_dependent BIT, is_ambiguous BIT
);
DECLARE @schema NVARCHAR(128), @name NVARCHAR(128);
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT SCHEMA_NAME(o.schema_id), o.name FROM sys.objects o
    WHERE o.type = 'P' AND o.is_ms_shipped = 0
      AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>);
OPEN cur;
FETCH NEXT FROM cur INTO @schema, @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        INSERT INTO @result
        SELECT @schema, @name,
            ISNULL(ref.referenced_schema_name, ''),
            ISNULL(ref.referenced_entity_name, ''),
            ISNULL(ref.referenced_minor_name, ''),
            ISNULL(ref.referenced_class_desc, ''),
            ISNULL(ref.is_selected, 0), ISNULL(ref.is_updated, 0),
            ISNULL(ref.is_select_all, 0), ISNULL(ref.is_insert_all, 0),
            ISNULL(ref.is_all_columns_found, 0),
            ISNULL(ref.is_caller_dependent, 0), ISNULL(ref.is_ambiguous, 0)
        FROM sys.dm_sql_referenced_entities(
            QUOTENAME(@schema) + '.' + QUOTENAME(@name), 'OBJECT'
        ) ref;
    END TRY
    BEGIN CATCH
    END CATCH
    FETCH NEXT FROM cur INTO @schema, @name;
END;
CLOSE cur; DEALLOCATE cur;
SELECT * FROM @result;
```

For views, change `o.type = 'P'` to `o.type = 'V'`. For functions, change to `o.type IN ('FN', 'IF', 'TF')`.

Process the results as follows:

1. **Group by referencing object.** For each proc/view/function, collect all referenced entities with their `is_selected`/`is_updated`/`is_insert_all` flags and column-level detail (`referenced_minor_name`).

2. **Classify referenced objects** by `referenced_class_desc`: `USER_TABLE` → tables, `VIEW` → views, `SQL_SCALAR_FUNCTION`/`SQL_TABLE_VALUED_FUNCTION`/`SQL_INLINE_TABLE_VALUED_FUNCTION` → functions, `SQL_STORED_PROCEDURE` → procedures. Default `OBJECT_OR_COLUMN` to tables.

3. **Write per-object catalog files:**
   - `catalog/procedures/<schema>.<proc>.json` with `references: {tables: [...], views: [...], functions: [...], procedures: [...]}`
   - `catalog/views/<schema>.<view>.json` with `references: {tables: [...], views: [...], functions: [...]}`
   - `catalog/functions/<schema>.<function>.json` with `references: {tables: [...], views: [...], functions: [...]}`

4. **Flip references** for table files: for each table referenced by a proc/view/function, add the referencing object to the table's `referenced_by` section in its catalog JSON file. Carry over `is_updated`/`is_selected`/`is_insert_all` and column-level detail.

Reference entry structure per proc/view/function file:

```json
{
  "references": {
    "tables": [
      {
        "schema": "HumanResources",
        "name": "Employee",
        "is_selected": false,
        "is_updated": true,
        "is_insert_all": false,
        "columns": [
          {"name": "BusinessEntityID", "is_selected": true, "is_updated": false}
        ]
      }
    ],
    "views": [],
    "functions": [],
    "procedures": [{"schema": "dbo", "name": "uspLogError", "is_selected": false, "is_updated": false}]
  }
}
```

Flipped `referenced_by` structure per table file:

```json
{
  "referenced_by": {
    "procedures": [
      {
        "schema": "dbo",
        "name": "usp_load_fact_sales",
        "is_selected": false,
        "is_updated": true,
        "is_insert_all": false,
        "columns": [
          {"name": "sale_id", "is_selected": true, "is_updated": false}
        ]
      }
    ],
    "views": [],
    "functions": []
  }
}
```

## Step 8 — Confirm

After all files are written, report a summary:

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

Tell the user they can now run `discover` or the `scoping-agent` against the output folder. The `discover refs` command will automatically use catalog data for instant writer identification.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. This is an inherent offline limitation of `sys.dm_sql_referenced_entities` — it resolves references at definition time, not runtime. These procs require LLM analysis via `discover show`.

## Step 9 — AST enrichment

Run the catalog enrichment script to fill DMF gaps:

```bash
uv run --project <shared-path> catalog-enrich --ddl-path <output-folder>
```

This augments catalog files with AST-derived references for:

- CTAS / SELECT INTO targets (DMF misses new table creation)
- TRUNCATE targets
- Indirect writers through EXEC call chains

Entries added carry `"detection": "ast_scan"` to distinguish from DMF-sourced data. Dynamic SQL (`EXEC(@sql)`, `sp_executesql`) remains unresolvable offline.

## Constraints

- Use `mssql:mssql-execute-sql` for all SQL Server queries — never use native tools to connect to the database directly.
- Use native Write tool for all local file writes — never route file output through MCP.
- The `{{.sql}}` parameter in the `mssql` MCP tool accepts arbitrary T-SQL. This is intentional for the controlled migration context. Do not pass user-supplied raw SQL strings through it without review.
- Do not log `SA_PASSWORD` or any connection string values.
