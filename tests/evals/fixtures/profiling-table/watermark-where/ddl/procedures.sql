-- ============================================================
-- SCENARIO: incremental load with WHERE watermark
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactProductSalesDelta
    @LastLoadDate DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @LastLoadDate IS NULL
        SET @LastLoadDate = DATEADD(DAY, -1, GETDATE());

    INSERT INTO silver.FactProductSalesDelta (
        ProductKey, CustomerKey, SalesAmount, OrderDate, ModifiedDate)
    SELECT
        d.ProductID,
        h.CustomerID,
        d.LineTotal,
        h.OrderDate,
        h.ModifiedDate
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE h.ModifiedDate > @LastLoadDate;
END;

GO
