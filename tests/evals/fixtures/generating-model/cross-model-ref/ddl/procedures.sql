-- ============================================================
-- SCENARIO: cross-model-ref — fact table joining bronze source
-- and an existing silver dbt model (DimCustomer)
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactSales
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.FactSales (CustomerKey, OrderDate, SalesAmount)
    SELECT
        dc.CustomerKey,
        so.OrderDate,
        so.TotalAmount
    FROM bronze.SalesOrder so
    JOIN silver.DimCustomer dc ON so.CustomerID = dc.CustomerAlternateKey;
END;

GO
