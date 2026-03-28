-- Scenario 1: single INSERT+SELECT — sqlglot parses as Create, refs extractable
-- Expected: writes_to=[silver.dimproduct], reads_from=[bronze.product], calls=[], parse_error=null
CREATE PROCEDURE [dbo].[usp_simple_insert]
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product
END
GO
-- Scenario 7: cross-DB 4-part reference in FROM — table.catalog != '' so filtered from reads_from
-- Single INSERT, no internal Commands → parse succeeds
-- Expected: writes_to=[silver.dimproduct], reads_from=[] (cross-DB excluded), calls=[], parse_error=null
CREATE PROCEDURE [dbo].[usp_cross_db]
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey)
    SELECT CAST(ProductID AS NVARCHAR(25)) FROM [OtherDB].[dbo].[ExternalProduct]
END
GO
