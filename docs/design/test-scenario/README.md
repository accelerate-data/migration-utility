# Test Scenario Design

## Decision

Use the existing `WideWorldImporters` (OLTP) and `WideWorldImportersDW` schemas as the test corpus.

- Treat `WideWorldImportersDW.Integration.*_Staging` as Bronze.
- Treat `WideWorldImportersDW.Dimension.*` and `WideWorldImportersDW.Fact.*` as Silver.
- Reuse existing `Integration.MigrateStaged*Data` procedures as the canonical staging-to-silver implementation.
- Build the missing Bronze load orchestration by running `Get*Updates` extraction logic and loading results into staging.

This keeps scenario design grounded in real SQL Server ETL logic and avoids synthetic table design.

## Scope

Cover all current DW dimensions and facts that are loaded by `MigrateStaged*Data`.

### Dimensions

- `Dimension.Date`
- `Dimension.City`
- `Dimension.Customer`
- `Dimension.Employee`
- `Dimension.[Payment Method]`
- `Dimension.[Stock Item]`
- `Dimension.Supplier`
- `Dimension.[Transaction Type]`

### Facts

- `Fact.Movement`
- `Fact.[Order]`
- `Fact.Purchase`
- `Fact.Sale`
- `Fact.[Stock Holding]`
- `Fact.[Transaction]`

## Load Patterns Covered By Existing Procedures

### Dimension patterns

- SCD Type 2 (close current row and insert new row version):
  - `MigrateStagedCityData`
  - `MigrateStagedCustomerData`
  - `MigrateStagedEmployeeData`
  - `MigrateStagedPaymentMethodData`
  - `MigrateStagedStockItemData`
  - `MigrateStagedSupplierData`
  - `MigrateStagedTransactionTypeData`

### Fact patterns

- Incremental upsert with `MERGE`:
  - `MigrateStagedMovementData`
- Partial replace with `DELETE + INSERT`:
  - `MigrateStagedOrderData`
  - `MigrateStagedPurchaseData`
  - `MigrateStagedSaleData`
- Full refresh with `TRUNCATE + INSERT`:
  - `MigrateStagedStockHoldingData`
- Append insert:
  - `MigrateStagedTransactionData`

### Shared behavior

- Dimension key resolution from natural IDs using lookup subqueries.
- Lineage and cutoff bookkeeping via `Integration.Lineage` and `Integration.[ETL Cutoff]`.

## Execution Design

1. Extract from OLTP (`WideWorldImporters`) using `Get*Updates` logic.
2. Load extracted rows into matching DW staging table (`Integration.*_Staging`).
3. Execute corresponding `MigrateStaged*Data` procedure.
4. Validate dimension/fact outputs and lineage/cutoff updates.

## Open Questions

The following scenario types are not represented by current `MigrateStaged*Data` procedures and need explicit product decisions:

1. Dimension Type 1 `MERGE` overwrite behavior.
2. Temp-table based transformations (`#temp`) in staging-to-silver procedures.
3. Dynamic SQL translation (`EXEC`/`sp_executesql`) in staging-to-silver procedures.
4. Cross-database references in migration scenarios.
5. Cursor-based row-by-row transformation procedures.
