-- ============================================================
-- SCENARIO: role-playing FKs — reseller sales with 3 date keys
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactResellerSales
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.FactResellerSales;
    INSERT INTO silver.FactResellerSales (
        ProductKey, OrderDateKey, ShipDateKey, DueDateKey,
        CustomerKey, OrderQuantity, UnitPrice,
        SalesAmount, TaxAmt, Freight)
    SELECT
        d.ProductID             AS ProductKey,
        CAST(FORMAT(h.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateKey,
        CAST(FORMAT(ISNULL(h.ShipDate, h.OrderDate), 'yyyyMMdd') AS INT) AS ShipDateKey,
        CAST(FORMAT(h.DueDate, 'yyyyMMdd') AS INT)   AS DueDateKey,
        h.CustomerID            AS CustomerKey,
        d.OrderQty              AS OrderQuantity,
        d.UnitPrice,
        d.LineTotal             AS SalesAmount,
        CAST(h.TaxAmt / NULLIF(COUNT(*) OVER (PARTITION BY h.SalesOrderID), 0) AS MONEY) AS TaxAmt,
        CAST(h.Freight / NULLIF(COUNT(*) OVER (PARTITION BY h.SalesOrderID), 0) AS MONEY) AS Freight
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID;
END;

GO
