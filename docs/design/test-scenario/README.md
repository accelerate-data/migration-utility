# Test Scenario Design

## Decision

Use `AdventureWorks2022` (OLTP) as bronze source data and a hand-authored `MigrationTest` database
with `bronze` and `silver` schemas as the shared test corpus for all agent integration tests.

- Bronze tables are copied from `AdventureWorks2022` using `SELECT TOP … INTO` to keep size
  manageable.
- Silver tables and stored procedures are authored explicitly to cover the MoSCoW scoping-agent
  scenarios — they are not derived from an existing DW schema.

This approach gives full control over scenario coverage without the complexity of reverse-engineering
an existing DW's ETL layer.

Previous design considered `WideWorldImporters`/`WideWorldImportersDW` as the corpus; that
approach was dropped because the existing procedures did not map cleanly to the required scoping
scenarios without significant modification.

## Scenario Coverage

Each silver table targets exactly one scoping-agent scenario. See the reference doc for the full
scenario table and instructions to publish or pull the pre-built image:
[Test Database Image](../../reference/test-db-image/README.md)

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

## Source Files

- Schema + procedures: `scripts/sql/create-migration-test-db.sql`
- Per-scenario input fixtures: `scripts/sql/test-fixtures/`
- Publish helper: `scripts/publish-test-db-image.sh`
