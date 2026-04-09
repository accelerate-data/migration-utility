# Periodic Snapshot Classification Guide

Use this guide whenever `profiling-signals.md` tentatively points to `fact_periodic_snapshot`. Both periodic snapshots and transaction facts commonly use `TRUNCATE + INSERT` full-reload patterns, so write pattern alone is insufficient. Apply the decision flowchart below before confirming.

## What is a Periodic Snapshot?

A periodic snapshot fact table records the **state or balance of business entities at regular, fixed intervals** (daily, weekly, monthly). The grain is always **one row per entity per time period** — even if no activity occurred during that period. Rows represent a point-in-time measurement, not a business event.

**Canonical examples:** inventory on-hand by product/warehouse/day; account balance by customer/month; headcount by department/month; AR aging balance by customer/week.

**Key property:** Measures are **semi-additive** — they can be summed across entity dimensions but NOT across time. Summing inventory on-hand across days gives a meaningless number.

## Decision Flowchart

```text
1. Does the fact table have a single snapshot date column?
   (snapshot_date, as_of_date, period_end_date, balance_date,
   month_end_date, reporting_date)
   │
   ├─ NO → If there are multiple FK date keys pointing to the
   │       same date dimension (role-playing), this is fact_transaction.
   │       See negative signals below.
   │
   └─ YES → Continue to step 2.

2. Does the source query join to a calendar / date-spine table
   to ensure every period is represented (even with no activity)?
   │
   ├─ YES → Strong periodic snapshot signal. Continue to step 3.
   │
   └─ NO  → Check source tables. If source is detail/event-level
            tables (OrderDetail, InvoiceLineItem, StockMovement),
            lean toward fact_transaction.

3. Does the source query use GROUP BY (entity_key + period)?
   Are measures balance/stock/level values (inventory, account_balance,
   headcount) rather than event flow (sales_amount, quantity_ordered)?
   │
   ├─ YES → fact_periodic_snapshot (high confidence)
   │
   └─ NO  → A TRUNCATE+INSERT without aggregation or calendar join
            is more likely fact_transaction.
```

## Primary Signals — Required Evidence

At least two of the following should be present to confirm `fact_periodic_snapshot`:

| Signal | Weight |
|---|---|
| Single snapshot date column (`snapshot_date`, `as_of_date`, `period_end_date`, `balance_date`, `month_end_date`, `reporting_date`) | Strong |
| Source query CROSS JOINs or LEFT JOINs to a calendar / date-spine table | Strong |
| Source query aggregates with `GROUP BY entity_key, period_date` | Strong |
| Measures are balance/stock/level semantics — semi-additive across time (`inventory_on_hand`, `account_balance`, `headcount`, `market_share`) | Strong |
| Source tables are operational status or balance tables (`Inventory`, `GeneralLedger`, `EmployeePayroll`, `AccountBalance`) | Medium |
| Grain is (entity + period): rows exist for all periods even when no activity | Medium |

## Negative Signals — Rule Out Periodic Snapshot

If any of these are true, the table is **not** a periodic snapshot:

| Negative signal | Correct classification |
|---|---|
| Multiple FK date keys referencing the same date dimension (role-playing: `OrderDateKey`, `ShipDateKey`, `DueDateKey` all → `DimDate`) | `fact_transaction` |
| Transaction / order / invoice ID is part of the grain | `fact_transaction` |
| Source tables are event-level detail tables (`SalesOrderDetail`, `InvoiceLineItem`, `StockMovement`, `PaymentTransaction`) | `fact_transaction` |
| No calendar / date-spine join and no `GROUP BY (entity + period)` in source SQL | `fact_transaction` |
| Measures are fully additive across all dimensions including time (`sales_amount`, `quantity_ordered`, `discount_amount`) | `fact_transaction` |
| Source query pulls raw rows directly with a date filter and no aggregation | `fact_transaction` |

## Measure Semantics: Semi-Additive vs. Fully Additive

This is the most reliable single indicator when write pattern is ambiguous.

| Measure type | Aggregates across time? | Examples | Table type |
|---|---|---|---|
| Balance / stock / level (semi-additive) | No — summing across days is meaningless | `inventory_on_hand`, `account_balance`, `headcount`, `ar_balance` | `fact_periodic_snapshot` |
| Flow / event (fully additive) | Yes — YTD totals make business sense | `sales_amount`, `quantity_ordered`, `discount_amount`, `freight` | `fact_transaction` |

## Role-Playing FK Date Keys vs. Snapshot Date Column

| Pattern | Table type |
|---|---|
| Two or more `INT`/`SMALLINT` FK columns referencing the **same** date dimension (`OrderDateKey`, `ShipDateKey`, `DueDateKey` all → `DimDate`). All populated simultaneously in one INSERT. | `fact_transaction` |
| One `DATE`/`DATETIME` or `INT` FK column representing the period end (`snapshot_date`, `as_of_date`). Part of the composite grain `(entity_key, snapshot_date)`. Driven by a calendar join. | `fact_periodic_snapshot` |

## Worked Examples

### FactResellerSales — Transaction Fact (NOT periodic snapshot)

```sql
TRUNCATE TABLE silver.FactResellerSales;
INSERT INTO silver.FactResellerSales (
    ProductKey, OrderDateKey, ShipDateKey, DueDateKey, CustomerKey,
    OrderQuantity, UnitPrice, SalesAmount, TaxAmt, Freight)
SELECT
    d.ProductID,
    CAST(FORMAT(h.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateKey,
    CAST(FORMAT(h.ShipDate,  'yyyyMMdd') AS INT) AS ShipDateKey,
    CAST(FORMAT(h.DueDate,   'yyyyMMdd') AS INT) AS DueDateKey,
    h.CustomerID, d.OrderQty, d.UnitPrice, d.LineTotal, ...
FROM bronze.SalesOrderHeader h
JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID;
```

**Classification: `fact_transaction`**

- Three role-playing FK date keys (`OrderDateKey`, `ShipDateKey`, `DueDateKey`) all → `DimDate`
- Sources are event-level tables (`SalesOrderHeader`, `SalesOrderDetail`)
- No date-spine or calendar join; no `GROUP BY (entity + period)`
- Measures (`SalesAmount`, `UnitPrice`, `Freight`) are fully additive flow metrics

### FactInventorySnapshot — True Periodic Snapshot

```sql
TRUNCATE TABLE silver.FactInventorySnapshot;
INSERT INTO silver.FactInventorySnapshot (
    snapshot_date, ProductKey, WarehouseKey,
    quantity_on_hand, reorder_point, units_in_transit)
SELECT
    cal.snapshot_date,
    p.ProductKey,
    w.WarehouseKey,
    SUM(i.QuantityOnHand) AS quantity_on_hand,
    MAX(i.ReorderPoint)   AS reorder_point,
    SUM(i.UnitsInTransit) AS units_in_transit
FROM silver.DimDate cal
CROSS JOIN silver.DimProduct p
CROSS JOIN silver.DimWarehouse w
LEFT JOIN bronze.Inventory i
    ON cal.snapshot_date = CAST(i.InventoryDate AS DATE)
    AND i.ProductID = p.ProductID
    AND i.WarehouseID = w.WarehouseID
WHERE cal.snapshot_date BETWEEN @StartDate AND @EndDate
GROUP BY cal.snapshot_date, p.ProductKey, w.WarehouseKey;
```

**Classification: `fact_periodic_snapshot`**

- Single `snapshot_date` column forming the time dimension of the grain
- Joins to `DimDate` calendar to drive period coverage
- `GROUP BY (snapshot_date, ProductKey, WarehouseKey)` — explicit composite grain
- Measures (`quantity_on_hand`, `units_in_transit`) are stock/balance values — semi-additive across time
- Source is an operational inventory balance table, not event-level detail
