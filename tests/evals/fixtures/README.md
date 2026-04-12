# Eval Fixtures

DDL project extracted from the MigrationTest Docker database. Source of truth: `scripts/sql/create-migration-test-db.sql` with background and scenario mapping in `scripts/sql/README.md`.

## Extraction

Extract once. Re-extract only when the MigrationTest schema changes.

```bash
# 1. Ensure Docker container is running with MigrationTest
docker ps | grep sql-test

# 2. Set MCP connection environment
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=MigrationTest
export SA_PASSWORD=<your-password>

# 3. Extract DDL and build catalog via setup-ddl
cd <migration-project-root>
claude --plugin-dir plugin/ -p "/setup-ddl"

# 4. Run AST enrichment
uv run --project plugin/lib catalog-enrich

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
