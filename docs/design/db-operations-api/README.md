# DB Operations API

Define one manifest-first runtime contract and one adapter-style database API so new technologies can be added without re-embedding workflow logic.

## Runtime Contract

`manifest.json` is the only runtime source of truth.

Runtime roles:

- `runtime.source`
- `runtime.target`
- `runtime.sandbox`

Rules:

- each role is fully independent
- no role inherits credentials from another role
- no role assumes another role's technology
- defaults in setup flows are only UX defaults, not persisted derivation rules
- consumers fail loudly when required runtime roles are missing instead of silently upgrading old manifest shapes

Each role stores:

- `technology`
- `dialect`
- `connection`
- optional `schemas`

Example:

```json
{
  "runtime": {
    "source": {
      "technology": "oracle",
      "dialect": "oracle",
      "connection": {
        "host": "localhost",
        "port": "1521",
        "service": "SRCPDB",
        "user": "kimball_src",
        "password_env": "ORACLE_SOURCE_PASSWORD",
        "schema": "bronze"
      }
    },
    "target": {
      "technology": "sql_server",
      "dialect": "tsql",
      "connection": {
        "host": "localhost",
        "port": "1433",
        "database": "MigrationTarget",
        "user": "sa",
        "password_env": "SA_PASSWORD"
      },
      "schemas": {
        "source": "bronze",
        "marts": "silver"
      }
    },
    "sandbox": {
      "technology": "oracle",
      "dialect": "oracle",
      "connection": {
        "host": "localhost",
        "port": "1521",
        "service": "SANDBOXPDB",
        "user": "kimball_sandbox",
        "password_env": "ORACLE_SANDBOX_PASSWORD",
        "schema": "bronze"
      }
    }
  },
  "extraction": {
    "schemas": ["bronze", "silver"],
    "extracted_at": "2026-04-13T00:00:00Z"
  }
}
```

For Oracle, source, target, and sandbox should use separate PDB or service endpoints.

## Layer Split

Technology-specific DB layer:

- low-level connectivity
- environment existence checks
- environment creation and teardown
- schema existence and creation
- fixture materialization
- fixture seeding
- procedure execution
- select execution
- compare execution

Source-agnostic orchestration layer:

- manifest validation
- desired state planning
- delta calculation
- logical source artifact generation
- target setup orchestration
- sandbox setup orchestration
- execution workflow orchestration

Read path:

- adapter reads backend state
- returns normalized data
- orchestration decides how to act

Write path:

- orchestration computes desired state
- adapter performs backend-specific writes

## Adapter API

Every supported technology should implement one adapter contract that can back source, target, and sandbox roles.

Core operations:

- `connect(role)`
- `environment_exists(role)`
- `ensure_environment(role)`
- `drop_environment(role)`
- `schema_exists(role, schema_name)`
- `ensure_schema(role, schema_name)`
- `ensure_source_schema(role, schema_name)`
- `list_source_tables(role, schema_name)`
- `create_source_table(role, table_name, columns)`
- `materialize_fixture(role, fixture_spec)`
- `seed_fixtures(role, fixtures)`
- `execute_procedure(role, scenario)`
- `execute_select(role, sql, fixtures)`
- `compare_two_sql(role, sql_a, sql_b, fixtures)`

Implementations should exist for:

- SQL Server
- Oracle

## Command Ownership

`/setup-ddl`

- collects source runtime
- extracts metadata
- does not own target or sandbox setup

`/setup-target`

- collects target runtime from the user
- owns target dbt setup
- owns target-side source schema setup
- reuses logical source artifact generation
- replaces `/init-dbt`

`/setup-sandbox`

- defaults sandbox technology to source technology
- allows user override
- collects full sandbox runtime independently

`generate-tests`

- consumes sandbox execution services

`refactoring-sql`

- consumes compare execution services

`generate-model`

- consumes target validation runtime services

`migrate-util ready`

- checks only readiness, never performs setup
- reports missing runtime roles and dbt scaffold files
- recommends `/setup-target` or `/setup-sandbox` when required runtime inputs are absent

## MigrationTest Fixture

`MigrationTest` is the canonical integration fixture name across supported technologies.

Rules:

- the configured database or service is only the container; the fixture identity is the `MigrationTest` schema inside that container
- each technology materializes the canonical fixture objects inside that one schema
- fixture objects do not use tier-specific schemas; tier semantics live in object names
- mutable runtime databases are generated on demand
- mutable database files are not committed
- fixture secrets come from env vars named in the manifest runtime roles; the manifest never stores secret values

Repo entrypoints:

- `scripts/sql/sql_server/materialize-migration-test.sh`
- `scripts/sql/oracle/materialize-migration-test.sh`

These shell scripts are env-driven and idempotent.

## Adding a New Technology

To add a new source or target technology:

1. extend the runtime technology-to-dialect mapping
2. implement the adapter contract
3. add `MigrationTest` materialization assets and shell entrypoint
4. wire the technology into setup flows
5. add unit, integration, and affected eval coverage
