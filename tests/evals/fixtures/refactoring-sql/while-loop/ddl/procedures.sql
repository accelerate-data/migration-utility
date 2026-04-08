-- ============================================================
-- SCENARIO: WHILE loop — iterative batch insert
-- ============================================================
CREATE PROCEDURE silver.usp_load_WhileLoopTarget
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @BatchSize INT = 1000;
    DECLARE @Offset INT = 0;
    DECLARE @RowCount INT = 1;

    TRUNCATE TABLE silver.WhileLoopTarget;

    WHILE @RowCount > 0
    BEGIN
        INSERT INTO silver.WhileLoopTarget (ProductAlternateKey, EnglishProductName)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName
        FROM bronze.Product
        ORDER BY ProductID
        OFFSET @Offset ROWS FETCH NEXT @BatchSize ROWS ONLY;

        SET @RowCount = @@ROWCOUNT;
        SET @Offset = @Offset + @BatchSize;
    END;
END;

GO
