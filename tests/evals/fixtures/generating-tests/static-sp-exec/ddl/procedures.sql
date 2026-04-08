-- ============================================================
-- SCENARIO: static sp_executesql — literal SQL string resolved
-- ============================================================
CREATE PROCEDURE silver.usp_load_StaticSpExecTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.StaticSpExecTarget;
    EXEC sp_executesql N'
        INSERT INTO silver.StaticSpExecTarget (ProductAlternateKey, EnglishProductName)
        SELECT CAST(ProductID AS NVARCHAR(25)), ProductName
        FROM bronze.Product';
END;

GO
