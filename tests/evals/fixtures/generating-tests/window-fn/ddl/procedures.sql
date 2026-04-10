-- ============================================================
-- SCENARIO: stage procedure — actual writer for FactInternetSales
-- ============================================================
CREATE PROCEDURE silver.usp_stage_FactInternetSales
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE [silver].[FactInternetSales];

    INSERT INTO [silver].[FactInternetSales] (
        SalesOrderNumber,
        SalesOrderLineNumber,
        ProductKey,
        CustomerKey,
        SalesTerritoryKey,
        OrderQuantity,
        UnitPrice,
        ExtendedAmount,
        SalesAmount,
        TaxAmt,
        Freight,
        OrderDate,
        DueDate,
        ShipDate
    )
    SELECT
        h.SalesOrderNumber,
        CAST(d.SalesOrderDetailID % 127 AS TINYINT) AS SalesOrderLineNumber,
        d.ProductID AS ProductKey,
        h.CustomerID AS CustomerKey,
        h.TerritoryID AS SalesTerritoryKey,
        d.OrderQty AS OrderQuantity,
        d.UnitPrice,
        d.LineTotal AS ExtendedAmount,
        d.LineTotal AS SalesAmount,
        h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS TaxAmt,
        h.Freight / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS Freight,
        h.OrderDate,
        h.DueDate,
        h.ShipDate
    FROM [bronze].[SalesOrderHeader] h
    INNER JOIN [bronze].[SalesOrderDetail] d
        ON h.SalesOrderID = d.SalesOrderID;
END;

GO

-- ============================================================
-- SCENARIO: resolved via call graph — ORCHESTRATOR (no direct write)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_FactInternetSales
AS
BEGIN
    SET NOCOUNT ON;
    EXEC silver.usp_stage_FactInternetSales;
END;

GO
