-- ============================================================
-- SCENARIO: WHILE — batched insert with loop counter
-- ============================================================
CREATE PROCEDURE silver.usp_load_WhileTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.WhileTarget;
    DECLARE @BatchId INT = 1;
    DECLARE @MaxBatch INT = (SELECT CEILING(COUNT(*) / 100.0) FROM bronze.Product);
    WHILE @BatchId <= @MaxBatch
    BEGIN
        INSERT INTO silver.WhileTarget (BatchId, ProductAlternateKey, EnglishProductName)
        SELECT
            @BatchId,
            CAST(ProductID AS NVARCHAR(25)),
            ProductName
        FROM bronze.Product
        ORDER BY ProductID
        OFFSET (@BatchId - 1) * 100 ROWS FETCH NEXT 100 ROWS ONLY;
        SET @BatchId = @BatchId + 1;
    END;
END;

GO
