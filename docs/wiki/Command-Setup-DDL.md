# Command: Setup DDL

## Purpose

Extracts DDL and catalog metadata from a live source system and writes local artifact files that downstream skills consume. Produces `manifest.json`, per-object DDL files in `ddl/`, and per-object catalog JSON files in `catalog/`. This is the first command in the pipeline and must complete before any discovery, scoping, or profiling work.

## Invocation

```text
/setup-ddl
```

No arguments. The command runs an interactive workflow that prompts for database and schema selection before extracting anything.

Trigger phrases: "set up DDL", "extract DDL from SQL Server", "populate DDL", "connect to the remote database and get DDL", "pull DDL from the source database".

## `extract` Subcommand (headless)

The `extract` subcommand runs the full extraction non-interactively — connect, query, assemble DDL, write catalog, enrich — in a single CLI call. It does not prompt for confirmation and does not write a `.staging/` directory.

```bash
uv run --project <shared-path> setup-ddl extract \
  --database <database> \
  --schemas dbo,silver \
  [--project-root .]
```

| Option | Required | Description |
|---|---|---|
| `--database` | SQL Server / Fabric only | Source database name. Not required for Oracle (connection is via `ORACLE_DSN`). |
| `--schemas` | yes | Comma-separated list of schemas to extract |
| `--project-root` | no | Defaults to current working directory |

`manifest.json` must already exist and contain a valid source technology before running `extract`. The extraction flow writes the active source endpoint to `runtime.source` and extraction metadata to `extraction.*`.

Enriched catalog fields (`scoping`, `profile`, `refactor`) written by earlier skill runs are preserved across re-extractions.

### Oracle prerequisites

- `oracledb` Python package: `uv pip install oracledb`
- Three environment variables: `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`
- The Oracle user must have `SELECT_CATALOG_ROLE` granted (required for `DBMS_METADATA.GET_DDL` and `ALL_*` views):

  ```sql
  GRANT SELECT_CATALOG_ROLE TO <user>;
  GRANT SELECT ANY DICTIONARY TO <user>;
  ```

### Oracle limitations

| Feature | Status |
|---|---|
| CDC tracking | Not supported — `cdc.json` is always empty |
| Change tracking | Not supported — `change_tracking.json` is always empty |
| Sensitivity classifications | Not supported — `sensitivity.json` is always empty |
| Column-level dependency flags (`is_selected`, `is_updated`, etc.) | Always `False` — `ALL_DEPENDENCIES` provides object-level references only |
| Auto-increment | Oracle 12c+ `IDENTITY` columns only — trigger-based sequences are not detected |
| Package-level procedures | Not extracted — `ALL_ARGUMENTS` is filtered to standalone procedures/functions |

## Prerequisites

- **`toolbox` binary on PATH** -- the `mssql` MCP server requires genai-toolbox. Run `toolbox --version` to verify. Install from `https://github.com/googleapis/genai-toolbox/releases` if missing.
- **Environment variables set** -- the SQL Server source flow reads these at startup:

  | Variable | Description | Example |
  |---|---|---|
  | `MSSQL_HOST` | SQL Server hostname or IP | `localhost` |
  | `MSSQL_PORT` | SQL Server port | `1433` |
  | `MSSQL_DB` | Default database (use `master` if no specific default) | `master` |
  | `SA_PASSWORD` | SQL login password | _(from env)_ |

- **Project root** -- the skill confirms `pwd` with the user before proceeding. If `manifest.json` already exists, the skill reads `runtime.source` and `extraction.schemas` from it and skips database selection.

## Pipeline

### Step 1 -- Select database (interactive)

Lists user databases (`database_id > 4`) via `sys.databases` and asks the user to pick one. Skipped when `manifest.json` already exists.

### Step 2 -- Select schemas (interactive)

Lists non-system schemas with object counts (tables, procedures, views, functions) and presents an `all` option. The user picks one or more schemas.

### Step 3 -- Extraction preview and confirm (interactive)

Runs count queries and catalog signal availability checks, then presents a summary:

```text
Extraction preview for [database]
Schemas: silver, gold

  Object counts:
    Tables:     42
    Procedures: 18
    Views:      5
    Functions:  3

  Catalog signals available:
    Primary keys:    38 constraints
    Foreign keys:    22 constraints
    Identity cols:   15 columns
    CDC-tracked:     2 tables

  DDL files will be written to:     ./ddl/
  Catalog files will be written to: ./catalog/
```

The user must confirm before any files are written.

### Step 4 -- Write manifest (deterministic)

```bash
uv run --project <shared-path> setup-ddl write-manifest \
  --technology sql_server \
  --database <database> \
  --schemas <comma-separated-schemas>
```

### Step 5 -- Export procedures, views, and functions (MCP + CLI)

For each object type, queries `OBJECT_DEFINITION()` via the `mssql` MCP tool, saves the result to `.staging/<type>.json`, then runs:

```bash
uv run --project <shared-path> setup-ddl assemble-modules \
  --input ./.staging/<type>.json \
  --type <procedures|views|functions>
```

### Step 6 -- Export tables (MCP + CLI)

Queries `sys.tables`, `sys.columns`, `sys.types`, and `sys.identity_columns`, saves to `.staging/table_columns.json`, then runs:

```bash
uv run --project <shared-path> setup-ddl assemble-tables \
  --input ./.staging/table_columns.json
```

### Step 7 -- Extract catalog signals and references (MCP + CLI)

Runs 12 catalog signal queries plus 3 DMF reference queries via MCP, saving each result to `.staging/`. Then processes all staging files in one pass:

```bash
uv run --project <shared-path> setup-ddl write-catalog \
  --staging-dir ./.staging \
  --database <database>
```

### Step 8 -- AST enrichment (deterministic)

```bash
uv run --project <shared-path> catalog-enrich --project-root .
```

Augments catalog files with AST-derived references that catalog queries miss: CTAS/SELECT INTO targets, TRUNCATE targets, and indirect writers through EXEC call chains. Entries carry `"detection": "ast_scan"`.

### Step 9 -- Report

Displays a summary of all extracted files and tells the user they can run `discover` or `scoping` next.

## Reads

This skill reads from a live SQL Server via the `mssql` MCP tool. No local catalog files are consumed.

## Writes

### `manifest.json`

| Field | Type | Required | Description |
|---|---|---|---|
| `schema_version` | string | yes | Always `"1.0"` |
| `technology` | string | yes | Primary project technology. Enum: `sql_server`, `oracle`, `duckdb`, `snowflake` |
| `dialect` | string | yes | Primary sqlglot dialect. Enum: `tsql`, `oracle`, `duckdb`, `snowflake` |
| `runtime.source` | object | yes | Active source runtime endpoint and connection information |
| `runtime.target` | object | no | Active target runtime endpoint, written later by `/setup-target` |
| `runtime.sandbox` | object | no | Active sandbox runtime endpoint, written later by `/setup-sandbox` |
| `extraction.schemas` | string[] | yes | List of schemas included in the extraction |
| `extraction.extracted_at` | string | yes | ISO 8601 timestamp of extraction |
| `init_handoff` | object | no | Validated prerequisite state (`env_vars`, `tools`, `timestamp`) written by `/init-ad-migration`. Required by all stage guards via `check_init_prerequisites` |

Technology-to-dialect mapping:

| Technology | Dialect | Delimiter |
|---|---|---|
| `sql_server` | `tsql` | `GO` |
| `oracle` | `oracle` | `/` |
| `duckdb` | `duckdb` | `;` |
| `snowflake` | `snowflake` | `;` |

### `ddl/` directory

| File | Contents |
|---|---|
| `ddl/tables.sql` | CREATE TABLE statements, one per table |
| `ddl/procedures.sql` | CREATE PROCEDURE statements |
| `ddl/views.sql` | CREATE VIEW statements |
| `ddl/functions.sql` | CREATE FUNCTION statements |

### `catalog/tables/<schema>.<table>.json`

Initial fields written by setup-ddl (profile and scoping sections are added later by other skills):

| Field | Type | Required | Description |
|---|---|---|---|
| `schema` | string | yes | Schema name |
| `name` | string | yes | Table name |
| `ddl_hash` | string | no | SHA-256 of normalized source DDL |
| `stale` | boolean | no | `true` when object was present in prior extraction but absent in latest |
| `columns` | array | no | Column definitions: `name`, `sql_type`, `is_nullable`, `is_identity` |
| `primary_keys` | array | yes | Declared PKs: `constraint_name`, `columns[]` |
| `unique_indexes` | array | yes | Unique indexes: `index_name`, `columns[]` |
| `foreign_keys` | array | yes | FK constraints: `constraint_name`, `columns[]`, `referenced_schema`, `referenced_table`, `referenced_columns[]` |
| `auto_increment_columns` | array | yes | Identity/auto-increment columns: `column`, `seed`, `increment`, `mechanism` |
| `change_capture` | object | no | CDC/change tracking: `enabled`, `mechanism` (enum: `cdc`, `change_tracking`, `stream`, `change_data_feed`) |
| `sensitivity_classifications` | array | no | PII labels: `column`, `label`, `information_type` |
| `referenced_by` | object | yes | Inbound references grouped by type (`procedures`, `views`, `functions`), each split into `in_scope` and `out_of_scope` |

### `catalog/procedures/<schema>.<proc>.json`

See [[Skill Analyzing Object]] for the full procedure catalog schema.

### Catalog signal staging files

The 12 catalog signal queries produce these staging files in `.staging/`:

| Staging file | Signal |
|---|---|
| `table_columns.json` | Column definitions with types, nullability, identity |
| `pk_unique.json` | Primary key and unique index constraints |
| `foreign_keys.json` | Foreign key relationships |
| `identity_columns.json` | Identity columns |
| `cdc.json` | CDC-tracked tables |
| `change_tracking.json` | Change tracking tables (graceful -- tolerates missing feature) |
| `sensitivity.json` | Sensitivity classifications (graceful) |
| `object_types.json` | Object type map (U, V, P, FN, IF, TF) |
| `definitions.json` | All proc/view/function definitions for routing flag scan |
| `proc_params.json` | Procedure parameters |
| `proc_dmf.json` | DMF references for procedures |
| `view_dmf.json` | DMF references for views |
| `func_dmf.json` | DMF references for functions |

## JSON Format

### `manifest.json` example

```json
{
  "schema_version": "1.0",
  "technology": "sql_server",
  "dialect": "tsql",
  "runtime": {
    "source": {
      "technology": "sql_server",
      "dialect": "tsql",
      "connection": {
        "host": "localhost",
        "port": "1433",
        "database": "AdventureWorksDW",
        "user": "sa",
        "driver": "FreeTDS"
      }
    }
  },
  "extraction": {
    "schemas": ["dbo", "silver", "gold"],
    "extracted_at": "2025-03-15T14:30:00Z"
  }
}
```

### `catalog/tables/silver.dimcustomer.json` example (initial)

```json
{
  "schema": "silver",
  "name": "DimCustomer",
  "columns": [
    { "name": "CustomerKey", "sql_type": "BIGINT", "is_nullable": false, "is_identity": true },
    { "name": "FirstName", "sql_type": "NVARCHAR(50)", "is_nullable": true, "is_identity": false },
    { "name": "Region", "sql_type": "NVARCHAR(50)", "is_nullable": true, "is_identity": false }
  ],
  "primary_keys": [
    { "constraint_name": "PK_DimCustomer", "columns": ["CustomerKey"] }
  ],
  "unique_indexes": [],
  "foreign_keys": [
    {
      "constraint_name": "FK_DimCustomer_Region",
      "columns": ["Region"],
      "referenced_schema": "silver",
      "referenced_table": "DimGeography",
      "referenced_columns": ["GeographyKey"]
    }
  ],
  "auto_increment_columns": [
    { "column": "CustomerKey", "seed": 1, "increment": 1, "mechanism": "identity" }
  ],
  "change_capture": { "enabled": true, "mechanism": "cdc" },
  "sensitivity_classifications": [
    { "column": "FirstName", "label": "Confidential", "information_type": "Name" }
  ],
  "referenced_by": {
    "procedures": {
      "in_scope": [
        { "schema": "silver", "name": "usp_load_DimCustomer", "is_selected": true, "is_updated": true, "detection": "catalog_query" }
      ],
      "out_of_scope": []
    },
    "views": { "in_scope": [], "out_of_scope": [] },
    "functions": { "in_scope": [], "out_of_scope": [] }
  }
}
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `toolbox: command not found` | genai-toolbox not installed | Install from `https://github.com/googleapis/genai-toolbox/releases` and add to PATH |
| `MSSQL_HOST is not set` | Missing source-side environment variable | Set `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, and `SA_PASSWORD` in `.env` or shell before extraction |
| `USE` statement not carrying across calls | Each MCP call is a discrete connection | Prepend `USE [<database>];` to every SQL block -- this is handled automatically by the skill |
| Change tracking query fails | Feature not enabled on server | Graceful degradation -- the query uses TRY/CATCH and saves an error marker instead of failing |
| Sensitivity classifications query fails | Feature not available (e.g., SQL Server edition) | Graceful degradation -- same TRY/CATCH pattern |
| DMF reference returns `ERROR:` rows | `sys.dm_sql_referenced_entities` cannot resolve a proc (e.g., missing dependent object) | Error rows are recorded in catalog; the skill continues with remaining objects |
| Dynamic SQL procs missing from `referenced_by` | `sys.dm_sql_referenced_entities` resolves at definition time, not runtime | Known limitation -- these procs require LLM analysis via [[Skill Analyzing Object]] |
| Re-running on existing project | `ddl/` and `catalog/` already exist | Skill warns and asks for confirmation -- re-run fully rebuilds both directories |
