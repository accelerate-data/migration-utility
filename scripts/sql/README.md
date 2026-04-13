# SQL Fixtures

`MigrationTest` is the canonical integration fixture across supported technologies.

## Materialization entrypoints

Each technology has an env-driven, idempotent shell entrypoint:

- `scripts/sql/sql_server/materialize-migration-test.sh`
- `scripts/sql/oracle/materialize-migration-test.sh`
- `scripts/sql/duckdb/materialize-migration-test.sh`

These scripts create or refresh the runtime fixture environment used by tests.
Mutable runtime databases are generated on demand and are not committed.

## SQL Server source-of-truth

`create-migration-test-db.sql` remains the editable SQL Server source for the
`MigrationTest` fixture database. It defines the SQL Server-specific schema and
data set that local SQL Server-backed tests and images are built from.

## Related artifacts

- SQL Server source: `scripts/sql/create-migration-test-db.sql`
- Eval fixture extraction target: `tests/evals/fixtures/migration-test/`
- Published SQL Server image build path: `scripts/publish-sqlserver-image.sh`
