-- ============================================================
-- SCENARIO: IF/ELSE — conditional branches write to same target
-- ============================================================
CREATE PROCEDURE silver.usp_load_IfElseTarget
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @HasRecords BIT;
    SELECT @HasRecords = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
    FROM silver.IfElseTarget;

    IF @HasRecords = 1
    BEGIN
        UPDATE tgt
        SET tgt.EnglishProductName = src.ProductName,
            tgt.ModifiedDate = GETDATE()
        FROM silver.IfElseTarget AS tgt
        INNER JOIN bronze.Product AS src
            ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25));
    END
    ELSE
    BEGIN
        INSERT INTO silver.IfElseTarget (ProductAlternateKey, EnglishProductName, ModifiedDate)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName,
            GETDATE()
        FROM bronze.Product;
    END;
END;

GO
