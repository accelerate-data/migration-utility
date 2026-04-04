# Stage 3 -- dbt Scaffolding

The `/init-dbt` command reads `manifest.json` and catalog data, asks you to pick a target platform, and scaffolds a complete dbt project with `sources.yml` generated from your catalog tables.

## Prerequisites

- `manifest.json` must exist (run `/setup-ddl` first)
- `catalog/tables/` must contain at least one `.json` file (warns if empty)

## Platform Selection

The command presents an explicit choice -- there is no default:

```text
Which target platform are you migrating to?

  1. Fabric Lakehouse (dbt-fabric)
  2. Spark (dbt-spark)
  3. Snowflake (dbt-snowflake)
  4. DuckDB (dbt-duckdb) -- development and CI testing only
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
| DuckDB | `dbt-duckdb` | `type: duckdb`, local file path |

### packages.yml

Includes `dbt-labs/dbt_utils` (>=1.0.0, <2.0.0).

### sources.yml

Generated from catalog table files. Every table in `catalog/tables/` becomes a source definition grouped by schema:

```yaml
version: 2

sources:
  - name: silver
    description: "Source tables from silver schema"
    tables:
      - name: DimCustomer
        description: "DimCustomer from source system"
      - name: FactInternetSales
        description: "FactInternetSales from source system"
```

## Validation

The command runs `dbt deps` to install packages, then `dbt compile` to validate the project. For non-DuckDB targets, a `dbt compile` failure due to placeholder credentials is expected -- the command tells you to update `profiles.yml` with real credentials.

## Idempotency

If `dbt/` already exists:

- Detects the existing project by checking for `dbt_project.yml`
- Regenerates `sources.yml` from current catalog (picks up new tables from a re-run of `/setup-ddl`)
- Never overwrites `profiles.yml` (you may have added real credentials)
- Never overwrites existing model files in `models/staging/` or `models/marts/`
- Re-runs `dbt deps` and `dbt compile`

## Next Step

Proceed to [[Stage 4 Sandbox Setup]] to create the throwaway test database, then [[Stage 1 Scoping]] to discover which stored procedures write to your target tables.
