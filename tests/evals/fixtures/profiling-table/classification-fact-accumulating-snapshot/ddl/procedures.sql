-- ============================================================
-- SCENARIO: accumulating snapshot — order fulfillment milestones
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactOrderFulfillment
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert new orders (OrderDate known, others NULL)
    INSERT INTO silver.FactOrderFulfillment (
        SalesOrderNumber, CustomerKey, ProductKey,
        OrderDate, OrderAmount)
    SELECT
        h.SalesOrderNumber,
        h.CustomerID,
        d.ProductID,
        h.OrderDate,
        d.LineTotal
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.FactOrderFulfillment f
        WHERE f.SalesOrderNumber = h.SalesOrderNumber);

    -- Update ship milestone
    UPDATE f
    SET f.ShipDate = h.ShipDate
    FROM silver.FactOrderFulfillment f
    JOIN bronze.SalesOrderHeader h ON f.SalesOrderNumber = h.SalesOrderNumber
    WHERE f.ShipDate IS NULL AND h.ShipDate IS NOT NULL;

    -- Update delivery milestone (estimated from ShipDate)
    UPDATE silver.FactOrderFulfillment
    SET DeliveryDate = DATEADD(DAY, 5, ShipDate)
    WHERE DeliveryDate IS NULL AND ShipDate IS NOT NULL;

    -- Update invoice milestone
    UPDATE silver.FactOrderFulfillment
    SET InvoiceDate = DeliveryDate
    WHERE InvoiceDate IS NULL AND DeliveryDate IS NOT NULL;
END;

GO
