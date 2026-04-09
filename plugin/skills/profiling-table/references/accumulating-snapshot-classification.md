# Accumulating Snapshot Classification Guide

Use this guide whenever `profiling-signals.md` tentatively points to `fact_accumulating_snapshot`. Apply the decision flowchart below before confirming the classification.

## What is an Accumulating Snapshot?

An accumulating snapshot fact table tracks a **single business process instance** (e.g., one sales order, one fulfillment) through multiple lifecycle stages. The **same row is updated** as each milestone completes. Rows are not immutable — they accumulate state over time.

## Decision Flowchart

```text
1. Does the writer procedure contain an UPDATE targeting the fact table?
   │
   ├─ NO → Cannot be accumulating snapshot. Go to step 4.
   │
   └─ YES → Continue to step 2.

2. Do the UPDATE statements target nullable date/datetime columns
   by checking WHERE col IS NULL AND source IS NOT NULL?
   │
   ├─ NO → Check what the UPDATEs target. If they target status flags
   │       or non-date columns only, this is not accumulating snapshot.
   │
   └─ YES → Continue to step 3.

3. Does the INSERT populate only the first milestone, leaving later
   milestone dates as NULL (to be filled by future UPDATEs)?
   │
   ├─ YES → fact_accumulating_snapshot (strong evidence)
   │
   └─ NO  → Re-examine. If all dates are inserted simultaneously,
            this may be a transaction fact, not accumulating snapshot.

4. If no UPDATE exists and the write pattern is TRUNCATE + INSERT:
   → fact_transaction (if row-level facts, no aggregation)
   → fact_periodic_snapshot (if measures represent a point-in-time balance)
   → fact_aggregate (if INSERT … SELECT … GROUP BY)
```

## Primary Signals (Write-Pattern) — Required Evidence

| Signal | Weight |
|---|---|
| Procedure contains both `INSERT` and `UPDATE` targeting the same fact table | Strong |
| Multiple sequential `UPDATE` blocks, each advancing a different nullable date column | Strong |
| Initial `INSERT` populates only the first milestone date; later milestone columns are `NULL` | Strong |
| `UPDATE` uses `WHERE milestone_date IS NULL AND source_date IS NOT NULL` pattern | Strong |

All primary signals must be absent before ruling out accumulating snapshot. A single missing element (e.g., no `UPDATE`) disqualifies the classification.

## Secondary Signals (Column-Shape) — Corroborating Only

These signals support the classification but are **not sufficient alone**.

| Signal | Notes |
|---|---|
| Multiple nullable `DATETIME`/`DATE` columns with stage-style names (`order_date`, `ship_date`, `delivery_date`, `invoice_date`) | Only applies when columns are actual date/datetime types — not integers |
| Natural business key column present alongside surrogate PK | Required for UPDATE targeting: you must identify which row to update |
| Later milestone columns are nullable; first milestone column is `NOT NULL` | Consistent with insert-first, update-later pattern |

## Negative Signals — Rule Out Accumulating Snapshot

If any of the following are true, the table is **not** an accumulating snapshot regardless of column names.

| Negative signal | Implication |
|---|---|
| `TRUNCATE` before `INSERT` | Full reload — history is destroyed each run; cannot accumulate |
| No `UPDATE` statement in the procedure | No milestone accumulation is possible |
| Date columns are `INT`/`SMALLINT` FK columns referencing a date dimension | Role-playing foreign keys, not milestone dates — see section below |
| All date columns are inserted simultaneously (none start NULL) | Transaction fact capturing a point-in-time event, not a lifecycle |

## Role-Playing FK Date Keys vs. Milestone Date Columns

This is the most common source of misclassification.

### Role-playing FK date keys (NOT accumulating snapshot)

- Column type: `INT` or `SMALLINT`
- Foreign key to a date dimension (e.g., `DimDate.DateKey`)
- Multiple columns reference the **same dimension table and key** (e.g., `OrderDateKey`, `ShipDateKey`, `DueDateKey` all → `DimDate.DateKey`)
- All populated **simultaneously** in a single `INSERT … SELECT`
- Write pattern: `TRUNCATE + INSERT` (full reload)
- Classification: `fact_transaction` or `fact_periodic_snapshot`

### True milestone date columns (accumulating snapshot)

- Column type: `DATETIME`, `DATE`, or `SMALLDATETIME`
- No FK constraint — stores the actual date value inline
- Later milestone columns are **nullable**, populated via `UPDATE` as each stage completes
- Write pattern: `INSERT` (first milestone only) + sequential `UPDATE` blocks
- Classification: `fact_accumulating_snapshot`

## Worked Examples

### FactResellerSales — Transaction Fact with Role-Playing Date FKs

```sql
-- Writer: TRUNCATE + INSERT (full reload)
TRUNCATE TABLE silver.FactResellerSales;
INSERT INTO silver.FactResellerSales (
    ProductKey, OrderDateKey, ShipDateKey, DueDateKey, ...
)
SELECT
    d.ProductID,
    CAST(FORMAT(h.OrderDate,  'yyyyMMdd') AS INT) AS OrderDateKey,   -- INT FK to DimDate
    CAST(FORMAT(h.ShipDate,   'yyyyMMdd') AS INT) AS ShipDateKey,    -- INT FK to DimDate
    CAST(FORMAT(h.DueDate,    'yyyyMMdd') AS INT) AS DueDateKey,     -- INT FK to DimDate
    ...
FROM bronze.SalesOrderHeader h ...
```

**Classification: `fact_transaction`**

- `TRUNCATE + INSERT` → full reload, no accumulation
- `OrderDateKey`, `ShipDateKey`, `DueDateKey` are `INT` FK columns to `DimDate` — role-playing date keys, not milestone dates
- All three date keys populated simultaneously in one INSERT
- No `UPDATE` statement

### FactOrderFulfillment — True Accumulating Snapshot

```sql
-- Step 1: INSERT new orders (only OrderDate populated; ShipDate, DeliveryDate, InvoiceDate are NULL)
INSERT INTO silver.FactOrderFulfillment (SalesOrderNumber, OrderDate, ShipDate, ...)
SELECT h.SalesOrderNumber, h.OrderDate, NULL, NULL, NULL ...
WHERE NOT EXISTS (SELECT 1 FROM silver.FactOrderFulfillment WHERE ...);

-- Step 2: UPDATE milestone — ShipDate
UPDATE f SET f.ShipDate = h.ShipDate
FROM silver.FactOrderFulfillment f ...
WHERE f.ShipDate IS NULL AND h.ShipDate IS NOT NULL;

-- Step 3: UPDATE milestone — DeliveryDate
UPDATE silver.FactOrderFulfillment
SET DeliveryDate = DATEADD(DAY, 5, ShipDate)
WHERE DeliveryDate IS NULL ...;

-- Step 4: UPDATE milestone — InvoiceDate
UPDATE silver.FactOrderFulfillment
SET InvoiceDate = DeliveryDate
WHERE InvoiceDate IS NULL ...;
```

**Classification: `fact_accumulating_snapshot`**

- INSERT then UPDATE on same table
- Each UPDATE advances exactly one nullable `DATETIME` milestone column
- Natural key (`SalesOrderNumber`) present for row identification
- Milestone columns start NULL and are populated incrementally
