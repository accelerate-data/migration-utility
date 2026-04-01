---
name: init-dbt
description: Scaffolds a dbt project with target adapter selection, sources.yml generation from catalog, and compile validation. Requires DDL extraction to be complete (manifest.json).
---

# Initialize dbt Project

Scaffold a complete dbt project for the migration target, generate sources from catalog, and validate with `dbt compile`.

## Step 1: Validate prerequisites

1. Check that `manifest.json` exists in the DDL artifacts directory (the project root or `$ARGUMENTS` if a path was provided). If missing, stop and tell the user to run `/setup-ddl` first.
2. Read `manifest.json` to confirm `technology` and `dialect` — this describes the **source** system, not the target.
3. Confirm a `catalog/tables/` directory exists with at least one `.json` file. If empty, warn that `sources.yml` will be empty.

## Step 2: Ask for target platform

Present the user with an explicit choice. There is no default — the user must pick one:

```text
Which target platform are you migrating to?

  1. Fabric Lakehouse (dbt-fabric)
  2. Spark (dbt-spark)
  3. Snowflake (dbt-snowflake)
  4. DuckDB (dbt-duckdb) — development and CI testing only

Enter your choice (1-4):
```

Ask the user and wait for a response. Do not proceed without an explicit selection.

## Step 3: Scaffold dbt project

Create the following structure at `{project-root}/dbt/`:

```text
dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   └── sources.yml
│   └── marts/
├── macros/
├── seeds/
└── tests/
```

### dbt_project.yml

```yaml
name: '<project_slug>'
version: '1.0.0'
config-version: 2

profile: '<project_slug>'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  <project_slug>:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

Derive `<project_slug>` from the directory name of the project root (lowercase, hyphens replaced with underscores).

### profiles.yml

Generate based on the selected target:

**Fabric Lakehouse (dbt-fabric):**

```yaml
<project_slug>:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      server: "<your-workspace>.datawarehouse.fabric.microsoft.com"
      port: 1433
      database: "<your-lakehouse>"
      schema: dbo
      authentication: CLI
      retries: 2
```

**Spark (dbt-spark):**

```yaml
<project_slug>:
  target: dev
  outputs:
    dev:
      type: spark
      method: odbc
      host: "<your-spark-host>"
      port: 443
      schema: default
      http_path: "<your-http-path>"
```

**Snowflake (dbt-snowflake):**

```yaml
<project_slug>:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<your-account>"
      user: "<your-user>"
      authenticator: externalbrowser
      database: "<your-database>"
      warehouse: "<your-warehouse>"
      schema: public
      threads: 4
```

**DuckDB (dbt-duckdb):**

```yaml
<project_slug>:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "target/dev.duckdb"
      threads: 4
```

### packages.yml

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

## Step 4: Generate sources.yml

Read every `.json` file in `catalog/tables/`. Group tables by schema. Generate `models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: <schema_name>
    description: "Source tables from <schema_name> schema"
    tables:
      - name: <table_name>
        description: "<table_name> from source system"
```

Each table file has `schema` and `name` fields (or derive from filename `<schema>.<name>.json`). Every table becomes a source definition regardless of scoping status.

## Step 5: Install and validate

Run:

```bash
cd <project-root>/dbt && dbt deps
```

Then:

```bash
cd <project-root>/dbt && dbt compile
```

If `dbt deps` fails, check whether `dbt` is installed and the adapter package is available. Use this mapping to tell the user which package to install:

| Target | Adapter package |
|---|---|
| Fabric Lakehouse | `dbt-fabric` |
| Spark | `dbt-spark` |
| Snowflake | `dbt-snowflake` |
| DuckDB | `dbt-duckdb` |

If `dbt compile` fails for non-DuckDB targets (due to placeholder credentials), that is expected. Tell the user to update `profiles.yml` with real credentials before running `dbt compile` again.

If `dbt compile` succeeds, report success.

## Step 6: Commit

If the project directory is a git repository:

```bash
git add dbt/
git commit -m "feat: scaffold dbt project for <target_platform>"
```

If not a git repo, skip silently.

## Step 7: Report

Present the result:

```text
dbt project scaffolded at <project-root>/dbt/

  Target:     <selected platform>
  Adapter:    <adapter package>
  Sources:    <N> tables across <M> schemas
  Validated:  dbt compile <passed|failed — see above>

Next steps:
  1. Update profiles.yml with your connection credentials (unless DuckDB)
  2. Run /scope, /profile, /generate-tests, and /generate-model to migrate stored procedures to dbt models
```

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `dbt deps` | non-zero | Adapter package missing. Tell user which package to install (see mapping above) |
| `dbt compile` | non-zero (non-DuckDB) | Expected if credentials are placeholders. Tell user to update `profiles.yml` |
| `dbt compile` | non-zero (DuckDB) | Real failure — surface error output |
| `git commit` | non-zero | Not a git repo or nothing to commit. Skip silently |

## Idempotency

If `dbt/` already exists:

1. Confirm `dbt_project.yml` exists — that is sufficient to detect an existing project.
2. Regenerate `sources.yml` from current catalog (may have new tables from a re-run of setup-ddl).
3. Do not overwrite `profiles.yml` (user may have added real credentials).
4. Do not overwrite existing model files in `models/staging/` or `models/marts/`.
5. Re-run `dbt deps` and `dbt compile`.
