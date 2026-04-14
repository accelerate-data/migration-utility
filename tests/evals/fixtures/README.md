# Eval Fixtures

Eval fixtures use the canonical `manifest.json` runtime contract:

- `runtime.source`
- `runtime.target` when the fixture includes dbt validation
- `runtime.sandbox` when the fixture exercises sandbox-backed execution
- `extraction.schemas`

Stale flat manifest fields are not supported in the eval harness.

Most SQL Server-backed fixtures still originate from the canonical `MigrationTest`
schema fixture inside the configured SQL Server container. Source-of-truth
background for the Kimball demo warehouse lives under `scripts/demo-warehouse/`,
and the SQL Server and Oracle materialization entrypoints live under
`tests/integration/sql_server/fixtures/` and `tests/integration/oracle/fixtures/`.

## Extraction

Extract once. Re-extract only when the `MigrationTest` schema contract changes.

```bash
# 1. Ensure the configured SQL Server container is running with the
#    canonical MigrationTest schema materialized
docker ps | grep sql-test

# 2. Set bootstrap source connection environment
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=<configured-source-database>
export SA_PASSWORD=<your-password>

# MigrationTest is the schema-level fixture contract inside that database.

# 3. Extract DDL and build catalog via setup-ddl
cd <migration-project-root>
claude --plugin-dir . -p "/setup-ddl"

# 4. Run AST enrichment
uv run --project lib catalog-enrich

# 5. Copy to fixture directory
cp -r . tests/evals/fixtures/migration-test/
```

## Structure

```text
migration-test/
  manifest.json
  ddl/
    tables.sql
    procedures.sql
    views.sql
  catalog/
    tables/<schema>.<table>.json
    procedures/<schema>.<proc>.json
    views/<schema>.<view>.json
```
