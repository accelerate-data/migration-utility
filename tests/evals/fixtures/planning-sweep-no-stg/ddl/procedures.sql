-- ============================================================
-- SCENARIO: insert-select
-- ============================================================
CREATE PROCEDURE silver.usp_load_InsertSelectTarget
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product;
END;

GO
