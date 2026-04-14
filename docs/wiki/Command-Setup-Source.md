# Command: setup-source

## Purpose

Validates credentials, extracts DDL and catalog metadata from a live source database, and writes
the artifact files that downstream skills consume. Produces `manifest.json`, per-object DDL files
in `ddl/`, and per-object catalog JSON files in `catalog/`. Must run before scoping, profiling,
or model generation.

## Invocation

```bash
ad-migration setup-source --technology sql_server --schemas silver,gold
ad-migration setup-source --technology oracle --schemas SH,HR
```

| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `sql_server` or `oracle` |
| `--schemas` | yes | Comma-separated list of schemas to extract |
| `--project-root` | no | Defaults to current working directory |
| `--no-commit` | no | Skip the automatic git commit after extraction |

## Prerequisites

The CLI validates all required environment variables before connecting. Missing variables cause an
immediate exit with a message listing exactly which vars are absent.

### SQL Server

| Variable | Description |
|---|---|
| `MSSQL_HOST` | SQL Server hostname or IP |
| `MSSQL_PORT` | SQL Server port (usually `1433`) |
| `MSSQL_DB` | Source database name |
| `SA_PASSWORD` | SQL Server password |

Also requires `toolbox` binary on PATH for live MCP extraction.

### Oracle

| Variable | Description |
|---|---|
| `ORACLE_HOST` | Oracle hostname or IP |
| `ORACLE_PORT` | Oracle listener port (usually `1521`) |
| `ORACLE_SERVICE` | Oracle service name |
| `ORACLE_USER` | Oracle username |
| `ORACLE_PASSWORD` | Oracle password |

Also requires SQLcl and Java 11+ installed.

## What it writes

| Path | Contents |
|---|---|
| `manifest.json` | Source runtime, extraction schemas, `extracted_at` timestamp |
| `ddl/tables.sql` | CREATE TABLE statements |
| `ddl/procedures.sql` | CREATE PROCEDURE statements |
| `ddl/views.sql` | CREATE VIEW statements |
| `ddl/functions.sql` | CREATE FUNCTION statements |
| `catalog/tables/<schema>.<table>.json` | Per-table catalog with columns, PKs, FKs, CDC, sensitivity |
| `catalog/procedures/<schema>.<proc>.json` | Per-procedure catalog |

## Re-running

Safe to re-run. Rebuilds `ddl/` and `catalog/` from source. Enriched catalog fields written by
earlier skill runs (`scoping`, `profile`, `refactor`) are preserved across re-extractions.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Error: missing required environment variables` | Credentials not set | Set the listed variables in `.envrc` and run `direnv allow` |
| `toolbox: command not found` | genai-toolbox not installed | Install from the genai-toolbox releases page and add to PATH |
| Exit code 2 | Connection error | Verify database is reachable, credentials are correct |
