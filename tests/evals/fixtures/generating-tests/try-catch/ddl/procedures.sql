-- ============================================================
-- SCENARIO: TRY/CATCH — load with error isolation
-- ============================================================
CREATE PROCEDURE silver.usp_load_TryCatchTarget
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO silver.TryCatchTarget (ProductAlternateKey, EnglishProductName)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName
        FROM bronze.Product
        WHERE ProductName IS NOT NULL;
    END TRY
    BEGIN CATCH
        RAISERROR('Load failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH
END;

GO
