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

## Step 5 — Confirm

After all files are written, report a summary:

```text
DDL extraction complete → <output-folder>/
Database: <database>
Schemas:  <selected-schemas>

  tables.sql     : N tables
  procedures.sql : N procedures
  views.sql      : N views
  functions.sql  : N functions
```

Tell the user they can now run `discover` or `scope` skills, or invoke the `scoping-agent` against the output folder.

## Constraints

- Use `mssql:mssql-execute-sql` for all SQL Server queries — never use native tools to connect to the database directly.
- Use native Write tool for all local file writes — never route file output through MCP.
- The `{{.sql}}` parameter in the `mssql` MCP tool accepts arbitrary T-SQL. This is intentional for the controlled migration context. Do not pass user-supplied raw SQL strings through it without review.
- Do not log `SA_PASSWORD` or any connection string values.
