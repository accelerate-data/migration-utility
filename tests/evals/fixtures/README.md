# Eval Fixtures

Eval fixtures use the canonical `manifest.json` runtime contract:

- `runtime.source`
- `runtime.target` when the fixture includes dbt validation
- `runtime.sandbox` when the fixture exercises sandbox-backed execution
- `extraction.schemas`

Stale flat manifest fields are not supported in the eval harness.

Most SQL Server-backed fixtures still originate from the `MigrationTest` fixture database. Source-of-truth background and materialization entrypoints live under `scripts/sql/`.

## Extraction

Extract once. Re-extract only when the MigrationTest schema changes.

```bash
# 1. Ensure Docker container is running with MigrationTest
docker ps | grep sql-test

# 2. Set bootstrap source connection environment
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=MigrationTest
export SA_PASSWORD=<your-password>

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
