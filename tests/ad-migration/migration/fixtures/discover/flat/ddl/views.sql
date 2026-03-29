-- View referencing two tables from reads_from
CREATE VIEW [silver].[vw_ProductCatalog]
AS
    SELECT
        p.ProductAlternateKey,
        p.EnglishProductName,
        s.ProductID
    FROM [silver].[DimProduct] AS p
    JOIN bronze.Product AS s ON p.ProductAlternateKey = CAST(s.ProductID AS NVARCHAR(25))
GO
