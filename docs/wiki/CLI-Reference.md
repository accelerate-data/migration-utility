# CLI Reference

The `ad-migration` CLI manages migration state — DDL extraction, catalog, sandbox, and pipeline resets. It is a deterministic tool: no LLM calls, no side effects beyond writing files and connecting to databases.

## Git workflow

Git is your responsibility. The CLI writes files; you decide when to commit them.

When a CLI command successfully mutates durable repo state, it ends with a reminder to review and commit those changes before continuing.

The CLI does **not**:

- create branches or worktrees
- stage files
- commit changes
- push branches
- open or update PRs
- clean up worktrees after merge

**Recommended flow:**

```bash
git checkout -b feature/migration-setup
# run your CLI commands
git add manifest.json ddl/ catalog/ dbt/
git commit -m "chore: initial migration setup"
git push origin feature/migration-setup
# open PR, review, merge to main
```

Each command's output section below lists exactly which files it writes, so you know what to stage manually.

## Installation

```bash
# Via Homebrew
brew tap accelerate-data/homebrew-tap
brew install ad-migration

# Dev (no install needed)
uv run --project lib ad-migration <command>
```

## Global options

| Option | Description |
|---|---|
| `--quiet` | Suppress all output except errors, intended for CI use |
| `--verbose`, `-v` | Show warnings and log output on stderr |
| `--version` | Print the installed CLI version and exit |
| `--help` | Show help for the top-level CLI or subcommand |

## Setup commands

### setup-source

Extract DDL and catalog from a live source database.

```bash
ad-migration setup-source --schemas silver,gold
ad-migration setup-source --schemas SH,HR
ad-migration setup-source --all-schemas --yes
```

| Option | Required | Description |
|---|---|---|
| `--schemas` | yes* | Comma-separated schema names |
| `--all-schemas` | yes* | Discover and extract all schemas |
| `--yes` | no | Skip confirmation for `--all-schemas` |
| `--project-root` | no | Defaults to current directory |

*One of `--schemas` or `--all-schemas` is required.

Technology comes from `manifest.json` as `runtime.source`, seeded by `/init-ad-migration`.

**Environment variables — SQL Server:**

| Variable | Description |
|---|---|
| `SOURCE_MSSQL_HOST` | Hostname or IP |
| `SOURCE_MSSQL_PORT` | Port (usually `1433`) |
| `SOURCE_MSSQL_DB` | Database name |
| `SOURCE_MSSQL_USER` | Username |
| `SOURCE_MSSQL_PASSWORD` | Password |
| `MSSQL_DRIVER` | Optional ODBC driver override; defaults to `FreeTDS` |

**Environment variables — Oracle:**

| Variable | Description |
|---|---|
| `SOURCE_ORACLE_HOST` | Hostname or IP |
| `SOURCE_ORACLE_PORT` | Port (usually `1521`) |
| `SOURCE_ORACLE_SERVICE` | Service name |
| `SOURCE_ORACLE_USER` | Username |
| `SOURCE_ORACLE_PASSWORD` | Password |

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
ad-migration setup-target
ad-migration setup-target --source-schema bronze
```

| Option | Required | Description |
|---|---|---|
| `--source-schema` | no | Source schema for `sources.yml` (default: `bronze`) |
| `--project-root` | no | Defaults to current directory |

Technology comes from `manifest.json` as `runtime.target`, seeded by `/init-ad-migration`.

**Environment variables — SQL Server:**

| Variable | Description |
|---|---|
| `TARGET_MSSQL_HOST` | Hostname or IP |
| `TARGET_MSSQL_PORT` | Port (usually `1433`) |
| `TARGET_MSSQL_DB` | Database name |
| `TARGET_MSSQL_USER` | Username |
| `TARGET_MSSQL_PASSWORD` | Password |

**Environment variables — Oracle:**

| Variable | Description |
|---|---|
| `TARGET_ORACLE_HOST` | Hostname or IP |
| `TARGET_ORACLE_PORT` | Port (usually `1521`) |
| `TARGET_ORACLE_SERVICE` | Service name |
| `TARGET_ORACLE_USER` | Username |
| `TARGET_ORACLE_PASSWORD` | Password |

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

**Environment variables — SQL Server sandbox:**

| Variable | Description |
|---|---|
| `SANDBOX_MSSQL_HOST` | Hostname or IP |
| `SANDBOX_MSSQL_PORT` | Port (usually `1433`) |
| `SANDBOX_MSSQL_USER` | Username |
| `SANDBOX_MSSQL_PASSWORD` | Password |

**Environment variables — Oracle sandbox:**

| Variable | Description |
|---|---|
| `SANDBOX_ORACLE_HOST` | Hostname or IP |
| `SANDBOX_ORACLE_PORT` | Port (usually `1521`) |
| `SANDBOX_ORACLE_SERVICE` | Service name |
| `SANDBOX_ORACLE_USER` | Admin username |
| `SANDBOX_ORACLE_PASSWORD` | Admin password |

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

Use this before `ad-migration setup-target` for any writerless table that should stay as a dbt source. `setup-target` reads the existing `is_source` decisions when generating `sources.yml` and creating target-side source tables.

To review the currently confirmed source tables later, run `/listing-objects list sources`.

If you add more source tables later, rerun `ad-migration setup-target`. It is idempotent and will apply only the newly required source-table changes.

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
