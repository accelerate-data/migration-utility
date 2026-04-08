-- ============================================================
-- SCENARIO: dynamic SQL via string concatenation then EXEC
-- ============================================================
CREATE PROCEDURE silver.usp_scope_execconcat
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql       NVARCHAR(MAX);
    DECLARE @target    NVARCHAR(128) = 'silver.ExecConcatTarget';
    DECLARE @src       NVARCHAR(128) = 'bronze.Product';

    SET @sql = 'INSERT INTO ' + @target + ' (ProductAlternateKey, EnglishProductName) '
             + 'SELECT CAST(ProductID AS NVARCHAR(25)), ProductName '
             + 'FROM ' + @src;

    EXEC(@sql);
END;

GO
