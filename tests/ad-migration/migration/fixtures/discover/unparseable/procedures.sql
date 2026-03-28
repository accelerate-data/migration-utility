-- Proc with IF/ELSE: sqlglot falls back to Command → DdlParseError raised
-- Expected: load_directory raises DdlParseError, discover CLI exits code 2
CREATE PROCEDURE [dbo].[usp_ConditionalLoad]
    @Mode INT = 0
AS
BEGIN
    IF @Mode = 1
    BEGIN
        INSERT INTO [silver].[DimProduct] (ProductAlternateKey)
        SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product
    END
    ELSE
    BEGIN
        UPDATE [silver].[DimProduct] SET EnglishProductName = 'Unknown' WHERE ProductKey = 0
    END
END
GO
