-- ============================================================
-- SCENARIO: dynamic SQL via concatenation — EXEC concatenation
-- ============================================================
CREATE PROCEDURE silver.usp_scope_ExecConcat
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO silver.ExecConcatTarget (ProductAlternateKey, EnglishProductName) '
             + N'SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product';

    EXEC(@sql);
END;

GO
