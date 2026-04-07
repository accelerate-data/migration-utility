---
name: init-dbt
description: Scaffolds a dbt project with target adapter selection, sources.yml generation from catalog, and compile validation. Requires DDL extraction to be complete (manifest.json).
user-invocable: true
argument-hint: "[project-root-path]"
---

# Initialize dbt Project

Scaffold a complete dbt project for the migration target, generate sources from catalog, and validate with `dbt compile`.

## Guards

- `manifest.json` must exist. If missing, stop and tell the user to run `/setup-ddl` first.
- All in-scope tables must have completed the analyze stage (scope resolved or no_writer_found). Run:

  ```bash
  uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util batch-plan
  ```

  If `scope_phase` in the output is non-empty, stop and tell the user: "N tables still need analyzing. Run `/analyzing-table <table>` on each one before initialising dbt." List the tables from `scope_phase`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track the automated phases of this command. After the user selects the target platform (Step 2) and before execution begins, create tasks for `Scaffold dbt project`, `Generate sources.yml`, `Install and validate`, and `Commit`. Update each task to `in_progress` when it starts and to `completed` or `cancelled` (include the error reason) when it finishes. Do not create tasks for interactive steps (platform selection).

## Step 1: Validate prerequisites

1. Read `manifest.json` to confirm `technology` and `dialect` — this describes the **source** system, not the target.
2. Confirm a `catalog/tables/` directory exists with at least one `.json` file. If empty, warn that `sources.yml` will be empty.

## Step 2: Ask for target platform

Present the user with an explicit choice. There is no default — the user must pick one:

```text
Which target platform are you migrating to?

  1. Fabric Lakehouse (dbt-fabric)
  2. Spark (dbt-spark)
  3. Snowflake (dbt-snowflake)
  4. SQL Server (dbt-sqlserver) — development, CI testing, and on-prem
  5. DuckDB (dbt-duckdb) — local development only

Enter your choice (1-5):
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

**SQL Server (dbt-sqlserver):**

```yaml
<project_slug>:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: "ODBC Driver 18 for SQL Server"
      server: "{{ env_var('MSSQL_HOST', 'localhost') }}"
      port: "{{ env_var('MSSQL_PORT', '1433') | int }}"
      database: "{{ env_var('MSSQL_DB') }}"
      user: sa
      password: "{{ env_var('SA_PASSWORD') }}"
      schema: dbo
      trust_cert: true
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

Run the deterministic CLI to generate and write `sources.yml`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" generate-sources --write --strict
```

This reads every `.json` file in `catalog/tables/`, includes only tables where `scoping.status == "no_writer_found"` (true external sources), and excludes tables with `scoping.status == "resolved"` (procedure targets that become dbt models via `{{ ref() }}`). The output JSON reports `included`, `excluded`, and `incomplete` table lists plus the written file path.

## Step 5: Install and validate

Run:

```bash
cd <project-root>/dbt && dbt deps
```

Then:

```bash
cd <project-root>/dbt && dbt compile
```

If `dbt deps` fails, check whether `dbt` is installed and the adapter package is available. Install dbt with the correct adapter using `uv tool`:

| Target | Install command |
|---|---|
| Fabric Lakehouse | `uv tool install dbt-core --with dbt-fabric` |
| Spark | `uv tool install dbt-core --with dbt-spark` |
| Snowflake | `uv tool install dbt-core --with dbt-snowflake` |
| SQL Server | `uv tool install dbt-core --with dbt-sqlserver --with pyodbc` |
| DuckDB | `uv tool install dbt-core --with dbt-duckdb` |

If `dbt compile` fails for Fabric/Spark/Snowflake targets (due to placeholder credentials), that is expected. Tell the user to update `profiles.yml` with real credentials before running `dbt compile` again. SQL Server uses `env_var()` for credentials — compile succeeds if MSSQL env vars are set.

If `dbt compile` succeeds, report success.

## Step 6: Commit

If the project directory is a git repository, check the current branch first:

```bash
git branch --show-current
```

If on `main`, notify the user:

> ⚠️ Committing dbt scaffold directly to `main`. dbt project initialisation is typically committed to main — this is expected. For migration work that follows, create a feature branch before running `/scope`, `/profile`, or other pipeline commands.

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
  1. Update profiles.yml with your connection credentials
     - Fabric/Spark/Snowflake: replace placeholder values with real credentials
     - SQL Server: credentials read from MSSQL_HOST, MSSQL_PORT, MSSQL_DB, SA_PASSWORD env vars (set during /init-ad-migration)
     - DuckDB: no credentials needed
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
2. Regenerate `sources.yml` by re-running `generate-sources --write --strict` (may have new tables or updated scoping results from a re-run of setup-ddl + analyzing-table).
3. Do not overwrite `profiles.yml` (user may have added real credentials).
4. Do not overwrite existing model files in `models/staging/` or `models/marts/`.
5. Re-run `dbt deps` and `dbt compile`.
