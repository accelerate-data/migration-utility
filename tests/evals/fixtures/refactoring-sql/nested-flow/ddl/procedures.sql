-- ============================================================
-- SCENARIO: Nested control flow — IF inside TRY/CATCH
-- ============================================================
CREATE PROCEDURE silver.usp_load_NestedFlowTarget
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        IF EXISTS (SELECT 1 FROM bronze.Product WHERE ProductName IS NOT NULL)
        BEGIN
            TRUNCATE TABLE silver.NestedFlowTarget;
            INSERT INTO silver.NestedFlowTarget (ProductAlternateKey, EnglishProductName, LoadMode)
            SELECT
                CAST(ProductID AS NVARCHAR(25)),
                ProductName,
                'full'
            FROM bronze.Product
            WHERE ProductName IS NOT NULL;
        END
        ELSE
        BEGIN
            MERGE INTO silver.NestedFlowTarget AS tgt
            USING (
                SELECT CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey, ProductName
                FROM bronze.Product
            ) AS src ON tgt.ProductAlternateKey = src.ProductAlternateKey
            WHEN MATCHED THEN
                UPDATE SET tgt.EnglishProductName = src.ProductName, tgt.LoadMode = 'delta'
            WHEN NOT MATCHED THEN
                INSERT (ProductAlternateKey, EnglishProductName, LoadMode)
                VALUES (src.ProductAlternateKey, src.ProductName, 'delta');
        END
    END TRY
    BEGIN CATCH
        RAISERROR('Load failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH
END;

GO
