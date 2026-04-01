---
name: setup-ddl
description: >
  This skill should be used when the user asks to "set up DDL", "extract DDL from SQL Server", "populate ddl", "connect to the remote database and get DDL", "pull DDL from the source database", or wants to initialise the local DDL artifact directory from a live SQL Server before running discovery or scoping.
---

# Setup DDL

Extract DDL from a live SQL Server and write local artifact files that the `ddl` MCP server used by `listing-objects`/`scoping-table` skills to read the schema.

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

## Preamble — confirm project root

1. Run `pwd` and show the resolved path. Use `AskUserQuestion` to ask: "Is this the correct project root?" If the user says no, tell them to `cd` to the correct directory and re-run the skill. Stop.

2. Check whether `manifest.json` exists in the current directory:

   - **Present** → read `source_database` and `extracted_schemas` from it. Show the user:

     ```text
     Project root locked to database: <source_database>
     Previously extracted schemas: <extracted_schemas>
     ```

     Then **skip Step 1** and proceed directly to **Step 2 — Select schemas**.

   - **Absent** → proceed to Step 1 as normal.

If `ddl/` or `catalog/` already exists in the project root, warn the user:

> Re-running will **fully rebuild** both `ddl/` and `catalog/`. All previously extracted files will be replaced.

Use `AskUserQuestion` to get confirmation before proceeding. If they decline, stop immediately.

## Workflow

Follow the step sequence below. Steps 1–3 are interactive (agent + MCP). Steps 4–8 use deterministic Python CLI tools — the agent saves MCP query results to `./.staging/` as JSON files, then calls the CLI tool to process them.

`<shared-path>` refers to `${CLAUDE_PLUGIN_ROOT}/../../lib`.

## Step 1 — Select database

List user databases on the server:

```sql
SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name
```

Use `AskUserQuestion` to present the list with a `None — exit` option. If the user picks `None`, stop immediately with no further action. Once a database is selected, run `USE [<database>]` before all subsequent queries to set the database context.

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

Use `AskUserQuestion` (with `multiSelect: true`) to present the results with an `all` option. If `all` is selected, do not add a schema filter to subsequent queries. Store the selected schemas for filtering in subsequent steps.

## Step 3 — Extraction preview + confirm

Run count queries and present a summary so the user knows what will be extracted **before any files are written**:

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

  DDL files will be written to:     ./ddl/
  Catalog files will be written to: ./catalog/
  Reference data from sys.dm_sql_referenced_entities will be extracted
  for all procedures, views, and functions.
```

Use `AskUserQuestion` to get confirmation before extraction proceeds. If they decline, stop immediately — no files are written.

## Step 4 — Write manifest

After user confirmation, write the manifest first (it only depends on database/schema selection):

```bash
uv run --project <shared-path> setup-ddl write-manifest \
  --technology sql_server \
  --database <database> \
  --schemas <comma-separated-schemas>
```

For Fabric Warehouse sources, use `--technology fabric_warehouse` instead.

Technology-to-dialect mapping:

| Technology | Dialect | Delimiter |
|---|---|---|
| `sql_server` | `tsql` | `GO` |
| `fabric_warehouse` | `tsql` | `GO` |
| `fabric_lakehouse` | `spark` | `;` |
| `snowflake` | `snowflake` | `;` |

## Step 5 — Export procedures, views, and functions

For each object type, run the query via `mssql:mssql-execute-sql`, save the result as JSON, then call the CLI tool.

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

Save the result to `./.staging/procedures.json`, then:

```bash
uv run --project <shared-path> setup-ddl assemble-modules \
  --input ./.staging/procedures.json \
  --type procedures
```

Repeat for **views** (change `o.type = 'P'` to `o.type = 'V'`, save as `views.json`, `--type views`) and **functions** (change to `o.type IN ('FN', 'IF', 'TF')`, save as `functions.json`, `--type functions`).

If a query returns no results, skip the staging file and CLI call for that type.

## Step 6 — Export tables

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

Save the result to `./.staging/table_columns.json`, then:

```bash
uv run --project <shared-path> setup-ddl assemble-tables \
  --input ./.staging/table_columns.json
```

## Step 7 — Extract catalog signals and references

Run all catalog queries via `mssql:mssql-execute-sql` and save each result to the staging directory. The CLI tool reads these files and writes all catalog JSON files in one pass.

### Staging files to create

Save each MCP query result as a JSON file in `./.staging/`:

| Staging file | Query |
|---|---|
| `table_columns.json` | Same result from Step 6 (already saved) |
| `pk_unique.json` | PKs and unique indexes (see query below) |
| `foreign_keys.json` | Foreign keys (see query below) |
| `identity_columns.json` | Identity columns (see query below) |
| `cdc.json` | CDC-tracked tables (see query below) |
| `change_tracking.json` | Change tracking tables (graceful, see query below) |
| `sensitivity.json` | Sensitivity classifications (graceful, see query below) |
| `object_types.json` | Object type map (see query below) |
| `definitions.json` | All proc/view/function definitions for routing flag scan (see query below) |
| `proc_params.json` | Procedure parameters (see query below) |
| `proc_dmf.json` | DMF refs for procedures (see query below) |
| `view_dmf.json` | DMF refs for views (see query below) |
| `func_dmf.json` | DMF refs for functions (see query below) |

### Catalog signal queries

**Primary keys and unique indexes** → `pk_unique.json`:

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

**Foreign keys** → `foreign_keys.json`:

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

**Identity columns** → `identity_columns.json`:

```sql
SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, c.name AS column_name
FROM sys.identity_columns c
JOIN sys.tables t ON t.object_id = c.object_id
WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
```

**CDC** → `cdc.json`:

```sql
SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
FROM sys.tables t
WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1
  AND SCHEMA_NAME(t.schema_id) IN (<selected-schemas>)
```

**Change tracking** (graceful) → `change_tracking.json`:

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

**Sensitivity classifications** (graceful) → `sensitivity.json`:

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

**Object type map** → `object_types.json`:

```sql
SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type
FROM sys.objects o
WHERE o.is_ms_shipped = 0
  AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
```

**All definitions** (for routing flag scan) → `definitions.json`:

```sql
SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name,
       OBJECT_DEFINITION(o.object_id) AS definition
FROM sys.objects o
WHERE o.type IN ('P', 'V', 'FN', 'IF', 'TF') AND o.is_ms_shipped = 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
```

**Procedure parameters** → `proc_params.json`:

```sql
SELECT
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name AS proc_name,
    p.name AS param_name,
    TYPE_NAME(p.user_type_id) AS type_name,
    p.max_length,
    p.precision,
    p.scale,
    p.is_output,
    p.has_default_value
FROM sys.parameters p
JOIN sys.objects o ON o.object_id = p.object_id
WHERE o.type = 'P' AND o.is_ms_shipped = 0 AND p.parameter_id > 0
  AND SCHEMA_NAME(o.schema_id) IN (<selected-schemas>)
ORDER BY schema_name, proc_name, p.parameter_id
```

### DMF reference queries

Use server-side cursors to batch all `sys.dm_sql_referenced_entities` calls into one result set per object type. Run for **procedures** → `proc_dmf.json`:

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

For **views** → `view_dmf.json`: change `o.type = 'P'` to `o.type = 'V'`.
For **functions** → `func_dmf.json`: change to `o.type IN ('FN', 'IF', 'TF')`.

### Run the CLI tool

Once all staging files are saved:

```bash
uv run --project <shared-path> setup-ddl write-catalog \
  --staging-dir ./.staging \
  --database <database>
```

The tool outputs JSON with counts: `{"tables": N, "procedures": N, "views": N, "functions": N}`.

## Step 8 — AST enrichment

Run the catalog enrichment script to fill catalog-query gaps:

```bash
uv run --project <shared-path> catalog-enrich --project-root .
```

This augments catalog files with AST-derived references for:

- CTAS / SELECT INTO targets (catalog queries miss new table creation)
- TRUNCATE targets
- Indirect writers through EXEC call chains

Entries added carry `"detection": "ast_scan"` to distinguish from catalog-query-sourced data. Dynamic SQL (`EXEC(@sql)`, `sp_executesql`) remains unresolvable offline.

## Step 9 — Report

After all files are written, report a summary:

```text
DDL extraction complete → ./
Database: <database>
Schemas:  <selected-schemas>

  DDL files (ddl/):
    tables.sql     : N tables
    procedures.sql : N procedures
    views.sql      : N views
    functions.sql  : N functions

  Catalog files (catalog/):
    tables/     : N files
    procedures/ : N files
    views/      : N files
    functions/  : N files

  manifest.json at ./manifest.json
```

Tell the user they can now run `discover` or the `scoping-agent` against the project root. The `discover refs` command will automatically use catalog data for instant writer identification.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. This is an inherent offline limitation of `sys.dm_sql_referenced_entities` — it resolves references at definition time, not runtime. These procs require LLM analysis via `discover show`.

## Constraints

- Use `mssql:mssql-execute-sql` for all SQL Server queries — never use native tools to connect to the database directly.
- Use the `setup-ddl` CLI tool for all data processing and file writing — never generate or run ad-hoc scripts, and never process query results inline. The agent's role is to run SQL via MCP, save results to `.staging/`, and call the CLI tool.
- The `{{.sql}}` parameter in the `mssql` MCP tool accepts arbitrary T-SQL. This is intentional for the controlled migration context. Do not pass user-supplied raw SQL strings through it without review.
- Do not log `SA_PASSWORD` or any connection string values.
