CREATE VIEW silver.vw_ActiveProducts
AS
SELECT
    ProductKey,
    EnglishProductName,
    Status,
    ListPrice
FROM silver.DimProduct
WHERE Status = 'Active';

GO
