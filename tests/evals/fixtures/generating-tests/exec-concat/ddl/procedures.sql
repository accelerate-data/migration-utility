-- ============================================================
-- SCENARIO: exec-concat — dynamic SQL via concatenation
-- ============================================================
CREATE PROCEDURE silver.usp_scope_ExecConcat
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);

    SET @sql = 'INSERT INTO silver.ExecConcatTarget (ProductAlternateKey, EnglishProductName) ' +
               'SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product';

    EXEC(@sql);
END;

GO
