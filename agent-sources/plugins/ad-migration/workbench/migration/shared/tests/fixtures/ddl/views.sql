CREATE VIEW [silver].[vw_DimProduct]
WITH SCHEMABINDING
AS
    SELECT ProductKey, ProductAlternateKey, EnglishProductName
    FROM silver.DimProduct
GO
