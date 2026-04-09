-- ============================================================
-- SCENARIO: dynamic SQL via variable — EXEC(@variable)
-- ============================================================
CREATE PROCEDURE silver.usp_scope_execvariable
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO silver.ExecVariableTarget (ProductAlternateKey, EnglishProductName) '
             + N'SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product';

    EXEC(@sql);
END;

GO
