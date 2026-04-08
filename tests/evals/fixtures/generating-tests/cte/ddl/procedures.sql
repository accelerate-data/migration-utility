-- ============================================================
-- SCENARIO: single CTE — INSERT via WITH clause (active filter)
-- ============================================================
CREATE PROCEDURE silver.usp_load_SingleCteTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.SingleCteTarget;
    WITH active_products AS (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName                     AS EnglishProductName
        FROM bronze.Product
        WHERE SellEndDate IS NULL
    )
    INSERT INTO silver.SingleCteTarget (ProductAlternateKey, EnglishProductName)
    SELECT ProductAlternateKey, EnglishProductName
    FROM active_products;
END;

GO
