WITH active_products AS (
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product
    WHERE SellEndDate IS NULL
)
SELECT ProductAlternateKey, EnglishProductName
FROM active_products
