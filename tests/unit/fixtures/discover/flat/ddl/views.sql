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

-- View with aggregation (GROUP BY + SUM/COUNT)
CREATE VIEW [silver].[vw_SalesSummary]
AS
    SELECT
        Region,
        COUNT(*) AS SaleCount,
        SUM(Amount) AS TotalAmount
    FROM bronze.Sales
    GROUP BY Region
GO

-- View with window function (ROW_NUMBER OVER)
CREATE VIEW [silver].[vw_RankedProducts]
AS
    SELECT
        ProductKey,
        EnglishProductName,
        ROW_NUMBER() OVER (ORDER BY EnglishProductName) AS RowNum
    FROM silver.DimProduct
GO
