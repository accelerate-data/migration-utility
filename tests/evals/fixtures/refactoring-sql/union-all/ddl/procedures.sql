-- ============================================================
-- SCENARIO: UNION ALL — preserve segmented branches
-- ============================================================
CREATE PROCEDURE silver.usp_load_UnionAllTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.UnionAllTarget;
    INSERT INTO silver.UnionAllTarget (ProductAlternateKey, EnglishProductName, Segment)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, 'red'
    FROM bronze.Product
    WHERE Color = 'Red'
    UNION ALL
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, 'other'
    FROM bronze.Product
    WHERE Color <> 'Red' OR Color IS NULL;
END;

GO
