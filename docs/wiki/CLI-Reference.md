# CLI Reference

The `ad-migration` CLI manages migration state — DDL extraction, catalog, sandbox, and pipeline resets. It is a deterministic tool: no LLM calls, no side effects beyond writing files and connecting to databases.

## Git workflow

Git is your responsibility. The CLI writes files; you decide when to commit them.

**Recommended flow:**

```bash
git checkout -b feature/migration-setup
# run your CLI commands
git add manifest.json ddl/ catalog/ dbt/
git commit -m "chore: initial migration setup"
git push origin feature/migration-setup
# open PR, review, merge to main
```

Each command's output section below lists exactly which files it writes, so you know what to stage.

## Installation

```bash
# Via Homebrew
brew tap accelerate-data/homebrew-tap
brew install ad-migration

# Dev (no install needed)
uv run --project lib ad-migration <command>
```

## Setup commands

### setup-source

Extract DDL and catalog from a live source database.

```bash
ad-migration setup-source --technology sql_server --schemas silver,gold
ad-migration setup-source --technology oracle --schemas SH,HR
ad-migration setup-source --technology sql_server --all-schemas --yes
```

| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `sql_server` or `oracle` |
| `--schemas` | yes* | Comma-separated schema names |
| `--all-schemas` | yes* | Discover and extract all schemas |
| `--yes` | no | Skip confirmation for `--all-schemas` |
| `--project-root` | no | Defaults to current directory |

*One of `--schemas` or `--all-schemas` is required.

**Environment variables — SQL Server:**

| Variable | Description |
|---|---|
| `MSSQL_HOST` | Hostname or IP |
| `MSSQL_PORT` | Port (usually `1433`) |
| `MSSQL_DB` | Database name |
| `SA_PASSWORD` | Password |

**Environment variables — Oracle:**

| Variable | Description |
|---|---|
| `ORACLE_HOST` | Hostname or IP |
| `ORACLE_PORT` | Port (usually `1521`) |
| `ORACLE_SERVICE` | Service name |
| `ORACLE_USER` | Username |
| `ORACLE_PASSWORD` | Password |

**Files written:**

```text
manifest.json
ddl/tables.sql
ddl/procedures.sql
ddl/views.sql
ddl/functions.sql
catalog/tables/<schema>.<table>.json
catalog/procedures/<schema>.<proc>.json
```

---

### setup-target

Scaffold the dbt project and generate `sources.yml` from the catalog.

```bash
ad-migration setup-target --technology snowflake
ad-migration setup-target --technology fabric --source-schema bronze
```

| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `fabric`, `snowflake`, or `duckdb` |
| `--source-schema` | no | Source schema for `sources.yml` (default: `bronze`) |
| `--project-root` | no | Defaults to current directory |

**Files written:**

```text
manifest.json
dbt/dbt_project.yml
dbt/profiles.yml
dbt/packages.yml
dbt/models/staging/sources.yml
```

---

### setup-sandbox

Provision a sandbox database cloned from the source.

```bash
ad-migration setup-sandbox --yes
```

| Option | Required | Description |
|---|---|---|
| `--yes` | no | Skip confirmation prompt |
| `--project-root` | no | Defaults to current directory |

**Files written:**

```text
manifest.json  (runtime.sandbox section updated)
```

---

### teardown-sandbox

Drop the sandbox database and clear it from the manifest.

```bash
ad-migration teardown-sandbox --yes
```

| Option | Required | Description |
|---|---|---|
| `--yes` | no | Skip confirmation prompt |
| `--project-root` | no | Defaults to current directory |

**Files written:**

```text
manifest.json  (runtime.sandbox section cleared)
```

---

## Pipeline state commands

### reset

Reset the pipeline state for one or more tables at a given stage, or wipe everything.

```bash
# Reset a specific stage for a table
ad-migration reset scope silver.DimCustomer --yes
ad-migration reset profile silver.DimCustomer silver.DimProduct --yes

# Global reset — returns project to post-setup-source state
ad-migration reset all --yes
```

| Option | Required | Description |
|---|---|---|
| `stage` | yes | `scope`, `profile`, `generate-tests`, `refactor`, or `all` |
| `fqns` | yes* | Fully-qualified table names (`schema.Table`) |
| `--yes` | no | Skip confirmation prompt |
| `--project-root` | no | Defaults to current directory |

*Not used for `all`.

**Files written — per-stage reset:**

```text
catalog/tables/<schema>.<table>.json  (stage section cleared)
```

**Files written — global reset:**

```text
catalog/  (deleted)
ddl/      (deleted)
manifest.json  (source/target/sandbox sections cleared)
```

---

### exclude-table

Mark tables as excluded from the migration pipeline.

```bash
ad-migration exclude-table silver.AuditLog silver.ChangeLog
```

| Option | Required | Description |
|---|---|---|
| `fqns` | yes | Fully-qualified table names |
| `--project-root` | no | Defaults to current directory |

**Files written:**

```text
catalog/tables/<schema>.<table>.json  (is_excluded: true)
```

---

### add-source-table

Confirm tables as dbt sources (`is_source: true`).

```bash
ad-migration add-source-table silver.DimGeography silver.DimCurrency
```

| Option | Required | Description |
|---|---|---|
| `fqns` | yes | Fully-qualified table names |
| `--project-root` | no | Defaults to current directory |

**Files written:**

```text
catalog/tables/<schema>.<table>.json  (is_source: true)
```

---

## Exit codes

| Code | Meaning | Example |
|---|---|---|
| `0` | Success | Command completed normally |
| `1` | Domain error | Missing env vars, no manifest, invalid arguments |
| `2` | IO / connection error | Database unreachable, credentials wrong |
