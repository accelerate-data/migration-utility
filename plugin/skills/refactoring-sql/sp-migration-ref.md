# Stored Procedure Migration Reference

Concise extraction rules for converting DML statements in stored procedures into pure SELECT statements. Source: [dbt Migrate from stored procedures](https://docs.getdbt.com/guides/migrate-from-stored-procedures).

## INSERT...SELECT

Extract the SELECT portion. The INSERT column list maps to SELECT aliases.

```sql
-- Original
INSERT INTO silver.DimCustomer (CustomerID, FullName)
SELECT CustomerID, FirstName + ' ' + LastName
FROM bronze.CustomerRaw WHERE IsActive = 1

-- Extracted SELECT
SELECT CustomerID, FirstName + ' ' + LastName AS FullName
FROM bronze.CustomerRaw WHERE IsActive = 1
```

Multiple INSERTs to the same target: combine with `UNION ALL`.

## MERGE

Extract the USING clause. The USING subquery contains the core transformation logic.

```sql
-- Original
MERGE INTO silver.DimCustomer AS tgt
USING (
    SELECT c.CustomerID, c.FirstName, g.Country
    FROM bronze.CustomerRaw c
    JOIN bronze.Geography g ON c.GeoKey = g.GeoKey
) AS src
ON tgt.CustomerID = src.CustomerID
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...

-- Extracted SELECT (the USING clause)
SELECT c.CustomerID, c.FirstName, g.Country
FROM bronze.CustomerRaw c
JOIN bronze.Geography g ON c.GeoKey = g.GeoKey
```

If the MERGE has both MATCHED (UPDATE) and NOT MATCHED (INSERT) with different column sets, extract the USING clause as-is. The materialization strategy (incremental with merge) handles the write semantics downstream.

## UPDATE

The SET clause becomes CASE expressions. Include all columns from the target, not just the updated ones.

```sql
-- Original
UPDATE silver.Orders
SET order_type = 'return'
WHERE total < 0

-- Extracted SELECT
SELECT
    OrderID,
    CASE WHEN total < 0 THEN 'return' ELSE order_type END AS order_type,
    total,
    order_date
FROM silver.Orders
```

When the UPDATE joins to another table:

```sql
-- Original
UPDATE o SET o.RegionName = g.RegionName
FROM silver.Orders o
JOIN bronze.Geography g ON o.GeoKey = g.GeoKey

-- Extracted SELECT
SELECT o.OrderID, g.RegionName, o.total, o.order_date
FROM silver.Orders o
JOIN bronze.Geography g ON o.GeoKey = g.GeoKey
```

## DELETE

Invert the WHERE clause to keep the rows that survive.

```sql
-- Original
DELETE FROM silver.Orders WHERE order_status IS NULL

-- Extracted SELECT
SELECT * FROM silver.Orders WHERE order_status IS NOT NULL
```

For soft-delete patterns, add a flag column:

```sql
SELECT *,
    CASE WHEN order_status IS NULL THEN 1 ELSE 0 END AS is_deleted
FROM silver.Orders
```

## Temp Table Chains

Each temp table becomes an inline subquery or CTE in the extracted SELECT.

```sql
-- Original
SELECT ... INTO #stage FROM bronze.Source WHERE ...
UPDATE #stage SET col = ... WHERE ...
INSERT INTO silver.Target SELECT ... FROM #stage

-- Extracted SELECT
SELECT ...,
    CASE WHEN ... THEN ... ELSE col END AS col
FROM bronze.Source
WHERE ...
```

## Cursor Loops

Rewrite as set-based operations. Cursors that accumulate running totals become window functions.

```sql
-- Original: cursor that computes running balance
-- Extracted SELECT
SELECT
    AccountID,
    TransactionDate,
    Amount,
    SUM(Amount) OVER (PARTITION BY AccountID ORDER BY TransactionDate) AS RunningBalance
FROM bronze.Transactions
```

## Dynamic SQL (sp_executesql)

Inline the constructed query. If the dynamic SQL is parameterized, replace parameters with their default values or column references.

## Key Principles

1. The extracted SELECT must produce the same columns and rows as the original DML would write to the target table
2. Preserve all JOINs, WHERE clauses, GROUP BY, and HAVING exactly
3. Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- dialect conversion happens later
4. The extracted SELECT is the baseline for equivalence comparison
