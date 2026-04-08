-- ============================================================
-- SCENARIO: periodic snapshot — daily inventory snapshot
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactInventorySnapshot
AS
BEGIN
    SET NOCOUNT ON;
    -- Full refresh for today's snapshot
    DELETE FROM silver.FactInventorySnapshot
    WHERE SnapshotDate = CAST(GETDATE() AS DATE);

    INSERT INTO silver.FactInventorySnapshot (
        ProductKey, WarehouseKey, SnapshotDate,
        UnitsOnHand, UnitsOnOrder, ReorderPoint, UnitCost)
    SELECT
        p.ProductID        AS ProductKey,
        1                  AS WarehouseKey,
        CAST(GETDATE() AS DATE) AS SnapshotDate,
        p.SafetyStockLevel AS UnitsOnHand,
        p.ReorderPoint     AS UnitsOnOrder,
        p.ReorderPoint,
        p.StandardCost     AS UnitCost
    FROM bronze.Product p;
END;

GO
