-- Simple: truncate + insert
CREATE PROCEDURE silver.usp_load_dimcustomer
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimCustomer;
    INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
    SELECT CustomerKey, FirstName FROM bronze.Customer;
END;
GO

-- Merge pattern
CREATE PROCEDURE silver.usp_load_factsales
AS
BEGIN
    SET NOCOUNT ON;
    MERGE INTO silver.FactSales AS tgt
    USING bronze.Sales AS src ON tgt.SalesKey = src.SalesKey
    WHEN MATCHED THEN UPDATE SET tgt.Amount = src.Amount
    WHEN NOT MATCHED THEN INSERT (SalesKey, CustomerKey, Amount)
        VALUES (src.SalesKey, src.CustomerKey, src.Amount);
END;
GO

-- CTE
CREATE PROCEDURE silver.usp_load_with_cte
AS
BEGIN
    WITH ranked AS (
        SELECT CustomerKey, FirstName,
               ROW_NUMBER() OVER (PARTITION BY Region ORDER BY CustomerKey) AS rn
        FROM bronze.Customer
    )
    INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
    SELECT CustomerKey, FirstName FROM ranked WHERE rn = 1;
END;
GO

-- Multi-level CTE
CREATE PROCEDURE silver.usp_load_with_multi_cte
AS
BEGIN
    WITH raw_orders AS (
        SELECT OrderID, CustomerID, TotalDue, OrderDate
        FROM bronze.SalesOrder
        WHERE OrderDate >= '2024-01-01'
    ),
    enriched AS (
        SELECT o.OrderID, o.TotalDue, c.FirstName, c.Region
        FROM raw_orders o
        JOIN bronze.Customer c ON o.CustomerID = c.CustomerKey
    ),
    aggregated AS (
        SELECT Region, SUM(TotalDue) AS RegionTotal, COUNT(*) AS OrderCount
        FROM enriched
        GROUP BY Region
    )
    INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
    SELECT RegionTotal, Region FROM aggregated;
END;
GO

-- CASE WHEN
CREATE PROCEDURE silver.usp_load_with_case
AS
BEGIN
    INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
    SELECT CustomerKey,
        CASE
            WHEN FirstName IS NULL THEN 'Unknown'
            ELSE FirstName
        END
    FROM bronze.Customer;
END;
GO

-- IF/ELSE with dual merge
CREATE PROCEDURE silver.usp_conditional_load
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM bronze.RunControl WHERE IsActive = 1)
    BEGIN
        MERGE INTO silver.DimCustomer AS tgt
        USING bronze.Customer AS src ON tgt.CustomerKey = src.CustomerKey
        WHEN MATCHED THEN
            UPDATE SET tgt.FirstName = src.FirstName
        WHEN NOT MATCHED THEN
            INSERT (CustomerKey, FirstName)
            VALUES (src.CustomerKey, src.FirstName);
    END
    ELSE
    BEGIN
        DELETE FROM silver.DimCustomer WHERE CustomerKey < 0;
    END
END;
GO

-- BEGIN TRY / CATCH
CREATE PROCEDURE silver.usp_try_catch_load
AS
BEGIN
    BEGIN TRY
        INSERT INTO silver.FactSales (SalesKey, Amount)
        SELECT SalesKey, Amount FROM bronze.Sales;
    END TRY
    BEGIN CATCH
        INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
        SELECT -1, ERROR_MESSAGE();
    END CATCH
END;
GO

-- Correlated subquery
CREATE PROCEDURE silver.usp_correlated_subquery
AS
BEGIN
    INSERT INTO silver.DimCustomer (CustomerKey, FirstName)
    SELECT o.CustomerID, 'Latest'
    FROM bronze.SalesOrder o
    WHERE o.OrderDate = (
        SELECT MAX(o2.OrderDate)
        FROM bronze.SalesOrder o2
        WHERE o2.CustomerID = o.CustomerID
    );
END;
GO

-- DELETE TOP
CREATE PROCEDURE silver.usp_cleanup
AS
BEGIN
    DELETE TOP (1000) FROM silver.FactSales
    WHERE Amount < 0;
END;
GO

-- Drop index + truncate + merge + create index
CREATE PROCEDURE silver.usp_full_reload
AS
BEGIN
    DROP INDEX IX_DimCustomer_Name ON silver.DimCustomer;
    TRUNCATE TABLE silver.DimCustomer;
    MERGE INTO silver.DimCustomer AS tgt
    USING bronze.Customer AS src ON tgt.CustomerKey = src.CustomerKey
    WHEN NOT MATCHED THEN
        INSERT (CustomerKey, FirstName) VALUES (src.CustomerKey, src.FirstName);
    CREATE INDEX IX_DimCustomer_Name ON silver.DimCustomer (FirstName);
END;
GO
