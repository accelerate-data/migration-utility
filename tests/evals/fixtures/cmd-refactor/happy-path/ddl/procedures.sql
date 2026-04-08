-- ============================================================
-- SCENARIO: INSERT INTO ... SELECT — simple full-refresh insert
-- ============================================================
CREATE PROCEDURE silver.usp_load_InsertSelectTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.InsertSelectTarget;
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product;
END;

GO

-- ============================================================
-- SCENARIO: UPDATE ... FROM JOIN — rewrite as source-driven select
-- ============================================================
CREATE PROCEDURE silver.usp_load_UpdateJoinTarget
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE tgt
    SET
        tgt.EnglishProductName = src.ProductName,
        tgt.LastSeenDate = GETDATE()
    FROM silver.UpdateJoinTarget AS tgt
    INNER JOIN bronze.Product AS src
        ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25));
END;

GO
