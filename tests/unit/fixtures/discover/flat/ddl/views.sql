-- View referencing two tables from reads_from
CREATE VIEW [silver].[vw_ProductCatalog]
AS
    SELECT
        p.ProductAlternateKey,
        p.EnglishProductName,
        s.ProductID
    FROM [silver].[DimProduct] AS p
    JOIN bronze.Product AS s ON p.ProductAlternateKey = CAST(s.ProductID AS NVARCHAR(25))
GO

-- View with aggregation (GROUP BY + SUM/COUNT)
CREATE VIEW [silver].[vw_SalesSummary]
AS
    SELECT
        Region,
        COUNT(*) AS SaleCount,
        SUM(Amount) AS TotalAmount
    FROM bronze.Sales
    GROUP BY Region
GO

-- View with window function (ROW_NUMBER OVER)
CREATE VIEW [silver].[vw_RankedProducts]
AS
    SELECT
        ProductKey,
        EnglishProductName,
        ROW_NUMBER() OVER (ORDER BY EnglishProductName) AS RowNum
    FROM silver.DimProduct
GO

-- View with CASE expression
CREATE VIEW [silver].[vw_CustomerTier]
AS
    SELECT
        CustomerKey,
        CASE WHEN TotalSpend > 10000 THEN 'Gold' WHEN TotalSpend > 1000 THEN 'Silver' ELSE 'Bronze' END AS Tier
    FROM bronze.Customer
GO

-- View with scalar subquery and EXISTS
CREATE VIEW [silver].[vw_ActiveCustomers]
AS
    SELECT
        CustomerKey,
        (SELECT MAX(OrderDate) FROM bronze.Orders o WHERE o.CustomerKey = c.CustomerKey) AS LastOrderDate
    FROM bronze.Customer c
    WHERE EXISTS (SELECT 1 FROM bronze.Orders o2 WHERE o2.CustomerKey = c.CustomerKey)
GO

-- View with CTE (single named CTE)
CREATE VIEW [silver].[vw_TopProducts]
AS
    WITH ranked AS (
        SELECT ProductKey, SUM(Amount) AS TotalSales
        FROM bronze.Sales
        GROUP BY ProductKey
    )
    SELECT ProductKey, TotalSales
    FROM ranked
    WHERE TotalSales > 1000
GO

-- View with multiple CTEs
CREATE VIEW [silver].[vw_SalesWithRegion]
AS
    WITH cte_sales AS (
        SELECT CustomerKey, SUM(Amount) AS Amount FROM bronze.Sales GROUP BY CustomerKey
    ),
    cte_region AS (
        SELECT CustomerKey, Region FROM bronze.Customer
    )
    SELECT s.CustomerKey, s.Amount, r.Region
    FROM cte_sales s
    JOIN cte_region r ON s.CustomerKey = r.CustomerKey
GO

-- Simple SELECT — no complex elements
CREATE VIEW [silver].[vw_SimpleCustomer]
AS
    SELECT CustomerKey, FirstName, LastName
    FROM bronze.Customer
GO

-- View with duplicate JOINs to same table (deduplication test)
CREATE VIEW [silver].[vw_DuplicateJoin]
AS
    SELECT a.CustomerKey, b.OrderKey, c.ProductKey
    FROM bronze.Customer a
    JOIN bronze.Orders b ON a.CustomerKey = b.CustomerKey
    JOIN bronze.Orders b2 ON b.OrderKey = b2.OrderKey
GO

-- View with combined elements: JOIN + GROUP BY + WINDOW
CREATE VIEW [silver].[vw_Combined]
AS
    SELECT
        c.Region,
        COUNT(*) AS OrderCount,
        SUM(o.Amount) AS TotalAmount,
        RANK() OVER (PARTITION BY c.Region ORDER BY SUM(o.Amount) DESC) AS RegionRank
    FROM bronze.Orders o
    JOIN bronze.Customer c ON o.CustomerKey = c.CustomerKey
    GROUP BY c.Region
GO
