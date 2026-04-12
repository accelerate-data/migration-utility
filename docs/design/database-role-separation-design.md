# Database Role Separation Design

## Decision

Separate SQL Server runtime configuration into explicit `source`, `target`, and `sandbox` roles.

- `source` is the existing warehouse state used for extract, catalog reads, procedure/view inspection, and sandbox cloning.
- `target` is the database context where dbt materializes and validates generated models.
- `sandbox` is always cloned from `source` and is never independently configured.

`MSSQL_DB` is removed as a supported catch-all database variable. The application must fail fast when required source or target configuration is missing.

## Goals

- Remove ambiguity between source reads, dbt target materialization, and sandbox execution.
- Make evals deterministic and isolated from ambient shell database settings.
- Give operators one clear mental model for how migration validation executes.

## Non-goals

- Backward compatibility for `MSSQL_DB`.
- Independent sandbox source selection.
- Changing business logic in migration, refactor, or dbt generation flows.

## Configuration Model

Add a repo-root `migration-config.yml` as the canonical runtime config file.

Recommended shape:

```yaml
source:
  type: sqlserver
  host: ${MSSQL_HOST}
  port: ${MSSQL_PORT:-1433}
  database: ${MIGRATION_SOURCE_DB}
  user: sa
  password: ${SA_PASSWORD}

target:
  type: sqlserver
  host: ${MSSQL_HOST}
  port: ${MSSQL_PORT:-1433}
  database: ${MIGRATION_TARGET_DB}
  user: sa
  password: ${SA_PASSWORD}

sandbox:
  clone_from_source: true
  database_prefix: __test_
```

Add a shared typed loader in `plugin/lib/shared/` that resolves config using this precedence:

1. explicit CLI flags
2. environment variables
3. `migration-config.yml`
4. code defaults

The loader is the only supported way to resolve SQL Server runtime roles.

## Execution Model

### Source

`source` is used for:

- extraction and discovery
- catalog enrichment inputs
- procedure/view/table inspection
- sandbox cloning

### Sandbox

The sandbox is an ephemeral execution arena cloned from `source`.

- Sandbox databases remain generated, e.g. `__test_*`
- Source-side scenario rows are seeded into sandbox source schemas
- Generated dbt artifacts are validated in the sandbox

### Target

`target` defines where dbt materializes relations during validation.

In evals and sandbox-backed validation:

- fixtures describe source inputs and expectations
- target relations are created by dbt materialization inside the sandboxed execution context
- target tables are not pre-seeded fixture outputs

This keeps validation end-to-end and prevents tests from passing against pre-existing target relations in a shared database.

## Eval Behavior

Eval fixtures are templates, not execution databases.

For each run:

1. copy the fixture into `tests/evals/output/runs/...`
2. clear volatile artifacts such as `dbt/logs` and `dbt/target`
3. resolve runtime config for that run
4. clone sandbox state from `source`
5. seed source-side fixture rows into the sandbox
6. run dbt against the sandbox target context
7. assert on freshly materialized relations

This means fixture-driven evals must not depend on ambient shell DB selection or pre-existing target tables in another database.

## Code Surfaces

### Shared runtime config

Add shared config modules under `plugin/lib/shared/`, likely:

- `runtime_config.py`
- `runtime_config_models.py`

Responsibilities:

- load and validate `migration-config.yml`
- apply environment and CLI overrides
- expose resolved `source`, `target`, and `sandbox` roles

### Sandbox layer

Update `plugin/lib/shared/sandbox/sql_server.py` so sandbox cloning always uses resolved `source.database`.

### CLI and context assembly

Update live-DB consumers to use resolved runtime config instead of direct `MSSQL_DB` reads. This likely affects:

- setup-ddl extract/discover paths
- test harness
- compare-sql
- migrate/refactor helpers that surface sandbox metadata

### dbt validation

Introduce a single resolution path for dbt target connection settings from `target`.

dbt validation must run against the writable workspace and resolved target context, not an ambient profile pointed at a shared database.

### Eval harness

Update `tests/evals/scripts/run-workspace-extension.js` and related prompt/eval plumbing so fixture-backed runs:

- receive resolved source/target config in the copied run workspace
- clear stale dbt state before execution
- run dbt validation in `{{run_path}}/dbt`

## Test Plan

### Unit

- config precedence resolution
- validation failures for missing source/target config
- sandbox cloning uses `source.database`
- dbt target resolution uses `target.database`
- eval workspace prep rewrites runtime config deterministically

### Integration

- sandbox clone comes from source DB
- source fixture rows seed into sandbox correctly
- dbt validation materializes into sandbox target context

### Eval

- one `generating-model` scenario
- one `reviewing-model` scenario
- one `refactoring-sql` or compare-sql scenario touching sandbox semantics

## Documentation Scope

Update user-facing docs because this changes setup and operator expectations:

- `docs/wiki`
- setup/reference docs
- init/scaffold templates
- any environment setup docs that currently describe a single SQL Server DB variable

Documentation must explain:

- source role
- target role
- sandbox derived from source
- eval behavior: fixture defines inputs, sandbox executes the run, dbt materializes target objects there

## Migration Strategy

Implement as a clean break.

- Remove support for `MSSQL_DB`
- Introduce explicit source/target config only
- Fail fast on ambiguous or missing runtime role configuration

## Rejected Alternatives

- Preserve `MSSQL_DB` as a compatibility alias
  - rejected because it keeps the ambiguity in place
- Fix eval fixtures only
  - rejected because the conflation exists in runtime and sandbox code too
- Make sandbox independently configurable
  - rejected because sandbox must always clone from source
