-- ============================================================
-- SCENARIO: SELECT INTO — full-refresh via SELECT INTO pattern
-- ============================================================
CREATE PROCEDURE silver.usp_load_SelectIntoTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.SelectIntoTarget;
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName
    INTO silver.SelectIntoTarget
    FROM bronze.Product;
END;

GO
