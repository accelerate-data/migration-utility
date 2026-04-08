-- ============================================================
-- SCENARIO: junk dimension — all flag combinations
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimSalesFlags
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimSalesFlags;
    INSERT INTO silver.DimSalesFlags (
        IsOnlineOrder, IsRushShipment, IsGiftWrapped,
        IsDiscounted, IsReturnable)
    SELECT DISTINCT
        h.OnlineOrderFlag,
        CASE WHEN h.ShipMethodID = 1 THEN 1 ELSE 0 END,
        0,
        CASE WHEN h.SubTotal <> h.TotalDue - h.TaxAmt - h.Freight THEN 1 ELSE 0 END,
        CASE WHEN h.Status IN (1, 2) THEN 1 ELSE 0 END
    FROM bronze.SalesOrderHeader h;
END;

GO
