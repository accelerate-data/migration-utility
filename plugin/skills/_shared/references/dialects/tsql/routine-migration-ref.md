# Routine Migration Reference — T-SQL

Concise extraction rules for converting DML statements in source routines into pure SELECT statements, with worked examples showing both the extracted SELECT and the refactored CTE version. Source: [dbt Migrate from stored procedures](https://docs.getdbt.com/guides/migrate-from-stored-procedures).

## INSERT...SELECT

Extract the SELECT portion. The INSERT column list maps to SELECT aliases.

### Extraction rule

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

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_InsertSelectTarget AS
BEGIN
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName
    FROM bronze.Product;
END

-- Extracted SELECT (sub-agent A)
SELECT
    CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
    ProductName AS EnglishProductName
FROM [bronze].[Product]

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM [bronze].[Product]
),
final AS (
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName
    FROM source_product
)
SELECT * FROM final
```

## MERGE

Extract the USING clause. The USING subquery contains the core transformation logic.

### Extraction rule

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

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_DimProduct AS
BEGIN
    MERGE silver.DimProduct AS tgt
    USING (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName AS EnglishProductName,
            StandardCost, ListPrice,
            ISNULL(Color, '') AS Color,
            Size, ProductLine, Class, Style,
            SellStartDate AS StartDate, SellEndDate AS EndDate,
            CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
                 WHEN SellEndDate IS NOT NULL THEN 'Outdated'
                 ELSE 'Current' END AS Status
        FROM bronze.Product
    ) AS src ON tgt.ProductAlternateKey = src.ProductAlternateKey
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...;
END

-- Extracted SELECT (sub-agent A)
SELECT
    CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
    ProductName AS EnglishProductName,
    StandardCost, ListPrice,
    ISNULL(Color, '') AS Color,
    Size, ProductLine, Class, Style,
    SellStartDate AS StartDate, SellEndDate AS EndDate,
    CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
         WHEN SellEndDate IS NOT NULL THEN 'Outdated'
         ELSE 'Current' END AS Status
FROM [bronze].[Product]

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM [bronze].[Product]
),
transformed_product AS (
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName,
        StandardCost, ListPrice,
        ISNULL(Color, '') AS Color,
        Size, ProductLine, Class, Style,
        SellStartDate AS StartDate, SellEndDate AS EndDate,
        CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
             WHEN SellEndDate IS NOT NULL THEN 'Outdated'
             ELSE 'Current' END AS Status
    FROM source_product
),
final AS (
    SELECT * FROM transformed_product
)
SELECT * FROM final
```

## UPDATE

The SET clause becomes CASE expressions or direct column references from the joined source. Include ALL columns from the target table, not just the updated ones.

### Extraction rule

Simple UPDATE with WHERE:

```sql
-- Original
UPDATE silver.Orders
SET order_type = 'return'
WHERE total < 0

-- Extracted SELECT: every target column, CASE for updated ones
SELECT
    OrderID,
    CASE WHEN total < 0 THEN 'return' ELSE order_type END AS order_type,
    total,
    order_date
FROM silver.Orders
```

UPDATE with JOIN to another table:

```sql
-- Original
UPDATE tgt SET tgt.RegionName = src.RegionName
FROM silver.Orders tgt
JOIN bronze.Geography src ON tgt.GeoKey = src.GeoKey

-- Extracted SELECT: target columns, joined source for SET values
SELECT tgt.OrderID, src.RegionName, tgt.total, tgt.order_date
FROM silver.Orders tgt
JOIN bronze.Geography src ON tgt.GeoKey = src.GeoKey
```

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_UpdateJoinTarget AS
BEGIN
    UPDATE tgt
    SET
        tgt.EnglishProductName = src.ProductName,
        tgt.LastSeenDate = GETDATE()
    FROM silver.UpdateJoinTarget AS tgt
    INNER JOIN bronze.Product AS src
        ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25));
END

-- Target columns: ProductAlternateKey, EnglishProductName, LastSeenDate

-- Extracted SELECT (sub-agent A)
-- All target columns. Updated columns use the SET source values.
-- Non-updated columns pass through from the target.
SELECT
    tgt.ProductAlternateKey,
    src.ProductName AS EnglishProductName,
    GETDATE() AS LastSeenDate
FROM [silver].[UpdateJoinTarget] AS tgt
INNER JOIN [bronze].[Product] AS src
    ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM [bronze].[Product]
),
existing_target AS (
    SELECT * FROM [silver].[UpdateJoinTarget]
),
updated AS (
    SELECT
        tgt.ProductAlternateKey,
        src.ProductName AS EnglishProductName,
        GETDATE() AS LastSeenDate
    FROM existing_target tgt
    INNER JOIN source_product src
        ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))
)
SELECT * FROM updated
```

## DELETE

Invert the WHERE clause to keep the rows that survive. The extracted SELECT returns the rows that would REMAIN after the DELETE.

### Extraction rule

```sql
-- Original
DELETE FROM silver.Orders WHERE order_status IS NULL

-- Extracted SELECT: invert condition, keep survivors
SELECT * FROM silver.Orders WHERE order_status IS NOT NULL
```

For equality conditions, invert with `<>` and handle NULLs:

```sql
-- Original
DELETE FROM silver.Target WHERE IsRetired = 1

-- Extracted SELECT: invert to keep non-retired, include NULLs
SELECT * FROM silver.Target WHERE IsRetired <> 1 OR IsRetired IS NULL
```

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_DeleteWhereTarget AS
BEGIN
    DELETE FROM silver.DeleteWhereTarget
    WHERE IsRetired = 1;
END

-- Target columns: ProductAlternateKey, EnglishProductName, IsRetired

-- Extracted SELECT (sub-agent A)
-- Invert the WHERE: keep rows where IsRetired is NOT 1
-- Must handle NULL (IS NULL means not retired)
SELECT
    ProductAlternateKey,
    EnglishProductName,
    IsRetired
FROM [silver].[DeleteWhereTarget]
WHERE IsRetired <> 1 OR IsRetired IS NULL

-- Refactored CTE (sub-agent B)
WITH all_records AS (
    SELECT * FROM [silver].[DeleteWhereTarget]
),
surviving AS (
    SELECT *
    FROM all_records
    WHERE IsRetired <> 1 OR IsRetired IS NULL
)
SELECT * FROM surviving
```

## TRUNCATE + INSERT (full reload)

TRUNCATE is a no-op for extraction — it just means "replace all rows". Extract only the INSERT...SELECT portion.

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_DimCustomer_Full AS
BEGIN
    TRUNCATE TABLE silver.DimCustomer;
    INSERT INTO silver.DimCustomer (
        CustomerAlternateKey, FirstName, MiddleName, LastName, Title,
        Gender, MaritalStatus, EmailPromotion, DateFirstPurchase)
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)),
        p.FirstName, p.MiddleName, p.LastName, p.Title,
        NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion,
        CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinOrderDate
        FROM bronze.SalesOrderHeader sh
        WHERE sh.CustomerID = c.CustomerID
    ) h;
END

-- Extracted SELECT (sub-agent A)
SELECT
    CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
    p.FirstName, p.MiddleName, p.LastName, p.Title,
    NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion,
    CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase
FROM [bronze].[Customer] c
JOIN [bronze].[Person] p ON c.PersonID = p.BusinessEntityID
OUTER APPLY (
    SELECT MIN(OrderDate) AS MinOrderDate
    FROM [bronze].[SalesOrderHeader] sh
    WHERE sh.CustomerID = c.CustomerID
) h

-- Refactored CTE (sub-agent B)
WITH source_customer AS (
    SELECT * FROM [bronze].[Customer]
),
source_person AS (
    SELECT * FROM [bronze].[Person]
),
source_orders AS (
    SELECT * FROM [bronze].[SalesOrderHeader]
),
customer_first_purchase AS (
    SELECT CustomerID, MIN(OrderDate) AS MinOrderDate
    FROM source_orders
    GROUP BY CustomerID
),
final AS (
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
        p.FirstName, p.MiddleName, p.LastName, p.Title,
        NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion,
        CAST(fp.MinOrderDate AS DATE) AS DateFirstPurchase
    FROM source_customer c
    JOIN source_person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN customer_first_purchase fp ON fp.CustomerID = c.CustomerID
)
SELECT * FROM final
```

## UNION ALL

Both extracted and refactored preserve the UNION ALL structure.

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_load_UnionAllTarget AS
BEGIN
    INSERT INTO silver.UnionAllTarget (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID <= 250
    UNION ALL
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID > 250;
END

-- Extracted SELECT (sub-agent A)
SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
    ProductName AS EnglishProductName, ISNULL(Color, '') AS Color
FROM [bronze].[Product] WHERE ProductID <= 250
UNION ALL
SELECT CAST(ProductID AS NVARCHAR(25)),
    ProductName, ISNULL(Color, '')
FROM [bronze].[Product] WHERE ProductID > 250

-- Refactored CTE (sub-agent B)
WITH low_products AS (
    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color
    FROM [bronze].[Product] WHERE ProductID <= 250
),
high_products AS (
    SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName, ISNULL(Color, '') AS Color
    FROM [bronze].[Product] WHERE ProductID > 250
),
final AS (
    SELECT * FROM low_products
    UNION ALL
    SELECT * FROM high_products
)
SELECT * FROM final
```

## Window Functions

Window functions (COUNT OVER, ROW_NUMBER, SUM OVER, etc.) are preserved as-is in both extracted and refactored SQL.

### Worked example

```sql
-- Original proc
CREATE PROCEDURE silver.usp_stage_FactInternetSales AS
BEGIN
    TRUNCATE TABLE silver.FactInternetSales;
    INSERT INTO silver.FactInternetSales (
        SalesOrderNumber, SalesOrderLineNumber, ProductKey, CustomerKey,
        SalesTerritoryKey, OrderQuantity, UnitPrice, ExtendedAmount,
        SalesAmount, TaxAmt, Freight, OrderDate, DueDate, ShipDate)
    SELECT
        h.SalesOrderNumber,
        CAST(d.SalesOrderDetailID % 127 AS TINYINT),
        d.ProductID, h.CustomerID, h.TerritoryID,
        d.OrderQty, d.UnitPrice,
        CAST(d.UnitPrice * d.OrderQty AS MONEY),
        d.LineTotal,
        CAST(h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY),
        CAST(h.Freight / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY),
        h.OrderDate, h.DueDate, h.ShipDate
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID;
END

-- Extracted SELECT (sub-agent A)
SELECT
    h.SalesOrderNumber,
    CAST(d.SalesOrderDetailID % 127 AS TINYINT) AS SalesOrderLineNumber,
    d.ProductID AS ProductKey,
    h.CustomerID AS CustomerKey,
    h.TerritoryID AS SalesTerritoryKey,
    d.OrderQty AS OrderQuantity,
    d.UnitPrice,
    CAST(d.UnitPrice * d.OrderQty AS MONEY) AS ExtendedAmount,
    d.LineTotal AS SalesAmount,
    CAST(h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS TaxAmt,
    CAST(h.Freight / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS Freight,
    h.OrderDate, h.DueDate, h.ShipDate
FROM [bronze].[SalesOrderHeader] h
JOIN [bronze].[SalesOrderDetail] d ON h.SalesOrderID = d.SalesOrderID

-- Refactored CTE (sub-agent B)
WITH source_header AS (
    SELECT * FROM [bronze].[SalesOrderHeader]
),
source_detail AS (
    SELECT * FROM [bronze].[SalesOrderDetail]
),
joined_sales AS (
    SELECT
        h.SalesOrderID,
        h.SalesOrderNumber,
        d.SalesOrderDetailID,
        d.ProductID, h.CustomerID, h.TerritoryID,
        d.OrderQty, d.UnitPrice, d.LineTotal,
        h.TaxAmt, h.Freight,
        h.OrderDate, h.DueDate, h.ShipDate
    FROM source_header h
    JOIN source_detail d ON h.SalesOrderID = d.SalesOrderID
),
final AS (
    SELECT
        SalesOrderNumber,
        CAST(SalesOrderDetailID % 127 AS TINYINT) AS SalesOrderLineNumber,
        ProductID AS ProductKey,
        CustomerID AS CustomerKey,
        TerritoryID AS SalesTerritoryKey,
        OrderQty AS OrderQuantity,
        UnitPrice,
        CAST(UnitPrice * OrderQty AS MONEY) AS ExtendedAmount,
        LineTotal AS SalesAmount,
        CAST(TaxAmt / COUNT(*) OVER (PARTITION BY SalesOrderID) AS MONEY) AS TaxAmt,
        CAST(Freight / COUNT(*) OVER (PARTITION BY SalesOrderID) AS MONEY) AS Freight,
        OrderDate, DueDate, ShipDate
    FROM joined_sales
)
SELECT * FROM final
```

## GROUPING SETS

Preserve GROUPING SETS, CUBE, and ROLLUP as-is in both outputs.

### Worked example

```sql
-- Original proc
CREATE PROCEDURE dbo.usp_GroupingSets AS
BEGIN
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL_COLORS'), CAST(COUNT(*) AS NVARCHAR(50))
    FROM bronze.Product
    GROUP BY GROUPING SETS ((Color), ());
END

-- Extracted SELECT (sub-agent A)
SELECT
    COALESCE(Color, 'ALL_COLORS') AS ConfigKey,
    CAST(COUNT(*) AS NVARCHAR(50)) AS ConfigValue
FROM [bronze].[Product]
GROUP BY GROUPING SETS ((Color), ())

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM [bronze].[Product]
),
color_groups AS (
    SELECT
        COALESCE(Color, 'ALL_COLORS') AS ConfigKey,
        CAST(COUNT(*) AS NVARCHAR(50)) AS ConfigValue
    FROM source_product
    GROUP BY GROUPING SETS ((Color), ())
)
SELECT * FROM color_groups
```

## PIVOT

PIVOT syntax is preserved in both outputs.

### Worked example

```sql
-- Original proc
CREATE PROCEDURE dbo.usp_Pivot AS
BEGIN
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT pvt.TerritoryGroup, CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50))
    FROM (
        SELECT st.TerritoryGroup, st.TerritoryID,
               CAST(st.SalesYTD AS MONEY) AS SalesAmt
        FROM bronze.SalesTerritory st
    ) src
    PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt;
END

-- Extracted SELECT (sub-agent A)
SELECT
    pvt.TerritoryGroup AS ConfigKey,
    CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50)) AS ConfigValue
FROM (
    SELECT st.TerritoryGroup, st.TerritoryID,
           CAST(st.SalesYTD AS MONEY) AS SalesAmt
    FROM [bronze].[SalesTerritory] st
) src
PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt

-- Refactored CTE (sub-agent B)
WITH source_territory AS (
    SELECT TerritoryGroup, TerritoryID,
           CAST(SalesYTD AS MONEY) AS SalesAmt
    FROM [bronze].[SalesTerritory]
),
pivoted AS (
    SELECT
        pvt.TerritoryGroup AS ConfigKey,
        CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50)) AS ConfigValue
    FROM source_territory
    PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt
)
SELECT * FROM pivoted
```

## Temp Table Chains

Each temp table becomes a CTE. UPDATE on a temp table becomes a CASE expression in the CTE.

### Extraction rule

```sql
-- Original
SELECT col1, col2 INTO #stage FROM bronze.Source WHERE ...
UPDATE #stage SET col2 = 'X' WHERE col1 > 10
INSERT INTO silver.Target SELECT col1, col2 FROM #stage

-- Extracted SELECT: inline the temp table + UPDATE as CASE
SELECT col1,
    CASE WHEN col1 > 10 THEN 'X' ELSE col2 END AS col2
FROM bronze.Source
WHERE ...

-- Refactored CTE
WITH staged AS (
    SELECT col1, col2
    FROM [bronze].[Source]
    WHERE ...
),
updated AS (
    SELECT col1,
        CASE WHEN col1 > 10 THEN 'X' ELSE col2 END AS col2
    FROM staged
)
SELECT * FROM updated
```

## Cursor Loops

Rewrite as set-based operations. Cursors that accumulate running totals become window functions.

### Extraction rule

```sql
-- Original: cursor that computes running balance row by row
-- Extracted SELECT: use window function
SELECT
    AccountID, TransactionDate, Amount,
    SUM(Amount) OVER (PARTITION BY AccountID ORDER BY TransactionDate) AS RunningBalance
FROM bronze.Transactions

-- Refactored CTE
WITH source_transactions AS (
    SELECT * FROM [bronze].[Transactions]
),
with_running_balance AS (
    SELECT
        AccountID, TransactionDate, Amount,
        SUM(Amount) OVER (PARTITION BY AccountID ORDER BY TransactionDate) AS RunningBalance
    FROM source_transactions
)
SELECT * FROM with_running_balance
```

## Dynamic SQL (sp_executesql)

Inline the constructed query. If the dynamic SQL is parameterized, replace parameters with their default values or column references.

## Key Principles

1. The extracted SELECT must produce the same columns and rows as the original DML would write to the target table
2. Preserve all JOINs, WHERE clauses, GROUP BY, and HAVING exactly
3. Keep source dialect syntax (ISNULL, CONVERT, etc.) -- dialect conversion happens later
4. The extracted SELECT is the baseline for equivalence comparison
5. The refactored CTE must produce the same result as the extracted SELECT
6. Every source table gets its own import CTE
7. Each logical CTE does one thing: join, filter, aggregate, or transform
8. The final CTE or SELECT produces all target table columns
