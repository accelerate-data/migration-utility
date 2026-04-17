# Integration Test Contract

Integration tests for live source platforms use a shared source-fixture contract:

- The source container is selected by platform-specific environment variables, such as a SQL Server database or an Oracle service/PDB.
- Each platform owns one canonical fixture schema inside that source container.
- `setup-ddl list-schemas` may inspect all visible schemas in the configured source container.
- `setup-ddl extract` integration tests pass only the canonical fixture schema.
- Extraction assertions validate canonical fixture objects only.
- Sandbox and test-harness integration tests clone or execute against only the canonical fixture schema.

The contract keeps broad discovery behavior covered without letting extraction tests depend on unrelated local schemas.

## Runtime Roles

Integration configuration is split by runtime role:

- `source` credentials list schemas and extract metadata from the source container.
- `sandbox` credentials create, reset, inspect, and drop temporary sandbox databases or schemas.
- `target` credentials materialize dbt source and seed objects in the target environment.

Source credentials must model customer extraction credentials. They must not default to server-owner users such as SQL Server `sa` or Oracle `sys`.

Sandbox credentials are allowed to be elevated because managed sandbox lifecycle creates and drops isolated runtime objects. Local Docker tests may use `sa` or `sys` for this role, but that is not the source-extraction contract.

The role-specific environment contract is a clean break. Integration tests and runtime helpers do not provide compatibility aliases for legacy env names; missing role-specific variables fail loudly with the missing role and variable names.

Fixture materialization is a test setup operation. It uses the sandbox/admin role to create or repair only the canonical fixture schema in the configured source container.

## Helper Shape

Integration bootstrapping stays explicit per technology. Shared code should provide only small primitives, such as role-specific environment validation and the `materialize_migration_test()` orchestration.

Do not introduce a generic integration platform class yet. Add that abstraction only after another source platform proves that the repeated code is stable across technologies.

## SQL Server Driver

SQL Server integration tests and generated runtime configuration use FreeTDS only. `MSSQL_DRIVER` is not part of the integration-test or customer runtime contract.

SQL Server role environment follows the same role split as other platforms:

- `SOURCE_MSSQL_*` selects the source database and source extraction credential.
- `SANDBOX_MSSQL_*` selects the sandbox host and sandbox lifecycle credential.
- `TARGET_MSSQL_*` selects the target host, database, and target materialization credential.

## Oracle Roles

Oracle schemas are users, so the fixture schema has its own source credential. Oracle tests still follow the same role split:

- `SOURCE_ORACLE_*` selects the source service/PDB, fixture schema, and source extraction credential.
- `SANDBOX_ORACLE_*` selects the sandbox service/PDB and sandbox lifecycle credential.
- `TARGET_ORACLE_*` selects the target service/PDB and target materialization credential.

Oracle `sys` is only a local Docker sandbox/admin default. It is not required for customer source discovery or extraction.

Target setup tests are outside this source-fixture contract. They may create target-side schemas such as `bronze` when validating dbt source or seed materialization.

## Adding A Technology

Every new source technology follows the same integration bootstrap template:

1. Add a `shared.dbops.<technology>` adapter and register it in `shared.dbops`.
2. Set the adapter fixture script path to `tests/integration/<technology>/fixtures/materialize.sh`.
3. Build fixture materialization env from the sandbox/admin role, not the source role.
4. Add idempotent fixture DDL and seed assets under `tests/integration/<technology>/fixtures/`.
5. Add a test helper that materializes the canonical fixture schema before live integration tests.
6. Add fixture materialization tests that prove the fixture schema is created or repaired when missing or stale.
7. Add `setup-ddl list-schemas` coverage that can see all visible schemas in the configured source container.
8. Add `setup-ddl extract` coverage that passes only the canonical fixture schema and validates only fixture objects.
9. Add sandbox or test-harness coverage that clones or executes against only the canonical fixture schema.
10. Use role-specific environment variables only; do not add compatibility aliases.

This checklist is the expected path for PostgreSQL, Fabric, and later source platforms.
