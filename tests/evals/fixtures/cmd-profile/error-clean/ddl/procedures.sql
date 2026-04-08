-- ============================================================
-- SCENARIO: resolved (direct writer via MERGE)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimProduct AS tgt
    USING (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName                     AS EnglishProductName,
            StandardCost,
            ListPrice,
            ISNULL(Color, '')               AS Color,
            Size,
            ProductLine,
            Class,
            Style,
            SellStartDate                   AS StartDate,
            SellEndDate                     AS EndDate,
            CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
                 WHEN SellEndDate IS NOT NULL      THEN 'Outdated'
                 ELSE 'Current' END          AS Status
        FROM bronze.Product
    ) AS src ON tgt.ProductAlternateKey = src.ProductAlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.EnglishProductName = src.EnglishProductName,
        tgt.StandardCost       = src.StandardCost,
        tgt.ListPrice          = src.ListPrice,
        tgt.Color              = src.Color,
        tgt.Size               = src.Size,
        tgt.ProductLine        = src.ProductLine,
        tgt.Class              = src.Class,
        tgt.Style              = src.Style,
        tgt.Status             = src.Status
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        ProductAlternateKey, EnglishProductName, StandardCost, ListPrice,
        Color, Size, ProductLine, Class, Style, StartDate, EndDate, Status)
    VALUES (
        src.ProductAlternateKey, src.EnglishProductName, src.StandardCost, src.ListPrice,
        src.Color, src.Size, src.ProductLine, src.Class, src.Style, src.StartDate, src.EndDate, src.Status);
END;

GO

-- ============================================================
-- SCENARIO: INSERT INTO ... SELECT — simple full-refresh insert
-- ============================================================
CREATE PROCEDURE silver.usp_load_InsertSelectTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.InsertSelectTarget;
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product;
END;

GO
