# SQL Fixtures

`MigrationTest` is the canonical integration fixture across supported technologies.

## Materialization entrypoints

Each technology has an env-driven, idempotent shell entrypoint:

- `scripts/sql/sql_server/materialize-migration-test.sh`
- `scripts/sql/oracle/materialize-migration-test.sh`

These scripts create or refresh the runtime fixture environment used by tests.
Mutable runtime databases are generated on demand and are not committed.

## Repo-managed fixture assets

Each supported technology keeps its `MigrationTest` materialization assets in
this tree:

- SQL Server: `scripts/sql/create-migration-test-db.sql`
- Oracle: `scripts/sql/oracle/migration_test.sql`

The shell entrypoints are the canonical setup path for tests. Runtime
databases are generated from these repo-managed assets and are not committed.

## Related artifacts

- SQL Server source: `scripts/sql/create-migration-test-db.sql`
- Oracle source: `scripts/sql/oracle/migration_test.sql`
- Eval fixture extraction target: `tests/evals/fixtures/migration-test/`
- Published SQL Server image build path: `scripts/publish-sqlserver-image.sh`
