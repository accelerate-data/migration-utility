# Backend Integration Parity

Backend integration coverage uses one canonical fixture contract: `MigrationTest`.

- SQL Server provisions `MigrationTest` as a schema inside the configured database.
- Oracle provisions `MIGRATIONTEST` as a schema inside the configured service or PDB.
- Canonical fixture objects live in that one schema for both backends.
- Tier semantics live in object names, not schema names.
- Shared integration tests live under `tests/integration/<backend>/<module>/`.
- DDL MCP keeps its own Python environment, but its live backend coverage also lives under `tests/integration/` and runs against the same `MigrationTest` fixture contract.

## Coverage Matrix

| Component | SQL Server | Oracle | Notes |
|---|---|---|---|
| `setup_ddl` | Covered | Covered | Shared CLI integration coverage under `tests/integration/<backend>/setup_ddl/`. |
| `fixture_materialization` | Covered | Covered | Verifies canonical `MigrationTest` materialization and idempotence. |
| `test_harness` | Covered | Covered | SQL Server includes `compare_two_sql`; Oracle covers sandbox lifecycle and scenario execution. |
| `ddl_mcp` | Not covered | Covered | Oracle live coverage runs from the DDL MCP env against `tests/integration/oracle/ddl_mcp/`. |
| `catalog_diff` | Covered | Not covered | SQL Server-only feature coverage today. |
| `catalog_enrich` | Not covered | Covered | Oracle-only feature coverage today. |
| `refactor` | Not covered | Not covered | No backend integration coverage yet. |
| `generate_tests` | Not covered | Not covered | No backend integration coverage yet. |
| `generate_model` | Not covered | Not covered | No backend integration coverage yet. |

## Execution

- Shared library backend tests run from `lib/`.
- DDL MCP backend tests run from `mcp/ddl/`.
- Both suites target top-level paths under `tests/integration/`.
