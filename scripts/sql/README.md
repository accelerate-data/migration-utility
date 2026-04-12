# SQL Fixtures

## MigrationTest Fixture Database

`create-migration-test-db.sql` is the source file for the `MigrationTest` SQL Server fixture database.

It exists to give the repo a controlled set of `bronze` and `silver` objects for scoping, profiling, extraction, and eval scenarios without depending on a customer warehouse.

### Why this database exists

- Bronze tables are copied from `AdventureWorks2022` so the fixture starts from realistic source data.
- Silver tables and procedures are hand-authored to cover migration-specific scenarios that are hard to get cleanly from an off-the-shelf warehouse sample.
- The published SQL Server Docker image bakes this database in so local test environments can be restored by pulling the image, but this SQL file remains the editable source-of-truth when the fixture needs to change.

### Scenario coverage

Each silver table targets a specific scoping scenario:

| Silver table | Scoping scenario | Expected status |
|---|---|---|
| `silver.DimProduct` | Direct MERGE writer | `resolved` |
| `silver.DimCustomer` | Two writers (Full + Delta) | `ambiguous_multi_writer` |
| `silver.FactInternetSales` | Orchestrator calls staging proc | `resolved` (call graph) |
| `silver.DimGeography` | No loader proc | `no_writer_found` |
| `silver.DimCurrency` | All writes via `sp_executesql` | `partial` |
| `silver.DimEmployee` | Callee references `[OtherDB]` | `error` (cross-db) |
| `silver.DimPromotion` | Writes through updateable view | `resolved` (writer-through-view) |
| `silver.DimSalesTerritory` | Indexed view as target | `resolved` (MV-as-target) |

### Related artifacts

- SQL source: `scripts/sql/create-migration-test-db.sql`
- Eval fixture extraction target: `tests/evals/fixtures/migration-test/`
- Published SQL Server image build path: `scripts/publish-sqlserver-image.sh`
