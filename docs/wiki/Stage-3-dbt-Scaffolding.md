# Stage 3 -- dbt Scaffolding

The `/init-dbt` command reads `manifest.json` and catalog data, asks you to pick a target platform, and scaffolds a complete dbt project with `sources.yml` generated from your catalog tables.

## Prerequisites

- `manifest.json` must exist (run `/setup-ddl` first)
- `catalog/tables/` must contain at least one `.json` file (warns if empty)
- All tables must have completed scoping (status `resolved` or `no_writer_found`). The command blocks if any table has incomplete scoping — run `/scope` or `/analyzing-table` for those tables first.

## Platform Selection

The command presents an explicit choice -- there is no default:

```text
Which target platform are you migrating to?

  1. Fabric Lakehouse (dbt-fabric)
  2. Spark (dbt-spark)
  3. Snowflake (dbt-snowflake)
  4. SQL Server (dbt-sqlserver) -- development, CI testing, and on-prem
  5. DuckDB (dbt-duckdb) -- development and CI testing only
```

Each platform generates a different `profiles.yml` with adapter-specific connection settings. DuckDB is intended for local development and CI testing only -- it uses a local file-based database at `target/dev.duckdb`.

## What It Produces

The command creates the following structure at `<project-root>/dbt/`:

```text
dbt/
  dbt_project.yml
  profiles.yml
  packages.yml
  models/
    staging/
      sources.yml
    marts/
  macros/
  seeds/
  tests/
```

### dbt_project.yml

Configured with the project slug derived from the directory name (lowercase, hyphens replaced with underscores). Default materializations:

- `staging/` models: `view`
- `marts/` models: `table`

### profiles.yml

Generated with placeholder credentials for the selected adapter. For non-DuckDB targets, you need to update this file with real connection details before running `dbt compile`.

| Target | Adapter package | Key settings |
|---|---|---|
| Fabric Lakehouse | `dbt-fabric` | `type: fabric`, CLI authentication, ODBC driver |
| Spark | `dbt-spark` | `type: spark`, ODBC method, HTTP path |
| Snowflake | `dbt-snowflake` | `type: snowflake`, external browser auth |
| SQL Server | `dbt-sqlserver` | `type: sqlserver`, SQL or Windows auth |
| DuckDB | `dbt-duckdb` | `type: duckdb`, local file path |

### packages.yml

Includes `dbt-labs/dbt_utils` (>=1.0.0, <2.0.0).

### sources.yml

Generated from catalog table files using the `generate-sources` CLI. Only tables where `scoping.status == "no_writer_found"` are included — these are true external sources that no stored procedure writes to. Tables with `scoping.status == "resolved"` (procedure write targets) are excluded because they will become dbt models referenced via `{{ ref() }}` rather than `{{ source() }}`.

```yaml
version: 2

sources:
  - name: silver
    description: "Source tables from silver schema"
    tables:
      - name: DimDate
        description: "DimDate from source system"
```

The `generate-sources` output lists included, excluded, and any incomplete tables so you can verify coverage before proceeding.

## Validation

The command runs `dbt deps` to install packages, then `dbt compile` to validate the project. For non-DuckDB targets, a `dbt compile` failure due to placeholder credentials is expected -- the command tells you to update `profiles.yml` with real credentials.

## Idempotency

If `dbt/` already exists:

- Detects the existing project by checking for `dbt_project.yml`
- Regenerates `sources.yml` from current catalog using the same `no_writer_found` filter (picks up tables scoped since the last run)
- Never overwrites `profiles.yml` (you may have added real credentials)
- Never overwrites existing model files in `models/staging/` or `models/marts/`
- Re-runs `dbt deps` and `dbt compile`

## Next Step

Proceed to [[Stage 4 Sandbox Setup]] to create the throwaway test database.
