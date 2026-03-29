-- Simple INSERT proc: references [silver].[DimProduct] using bracket notation
-- Expected: parseable, refs finds silver.dimproduct via bracket notation
CREATE PROCEDURE [dbo].[usp_LoadDimProduct]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product
END
GO
-- MERGE pattern
CREATE PROCEDURE [dbo].[usp_MergeDimProduct]
AS
BEGIN
    SET NOCOUNT ON;
    MERGE INTO [silver].[DimProduct] AS tgt
    USING bronze.Product AS src ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))
    WHEN MATCHED THEN
        UPDATE SET tgt.EnglishProductName = src.ProductName
    WHEN NOT MATCHED THEN
        INSERT (ProductAlternateKey, EnglishProductName)
        VALUES (CAST(src.ProductID AS NVARCHAR(25)), src.ProductName);
END
GO
-- CTE
CREATE PROCEDURE [dbo].[usp_LoadWithCTE]
AS
BEGIN
    WITH ranked AS (
        SELECT ProductID, ProductName,
               ROW_NUMBER() OVER (ORDER BY ProductID) AS rn
        FROM bronze.Product
    )
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM ranked WHERE rn <= 100;
END
GO
-- Multi-level CTE with subquery
CREATE PROCEDURE [dbo].[usp_LoadWithMultiCTE]
AS
BEGIN
    WITH base AS (
        SELECT ProductID, ProductName FROM bronze.Product
    ),
    filtered AS (
        SELECT b.ProductID, b.ProductName
        FROM base b
        WHERE b.ProductID NOT IN (
            SELECT CAST(ProductAlternateKey AS INT)
            FROM [silver].[DimProduct]
        )
    )
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM filtered;
END
GO
-- CASE WHEN
CREATE PROCEDURE [dbo].[usp_LoadWithCase]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)),
        CASE
            WHEN ProductName IS NULL THEN 'Unknown'
            ELSE ProductName
        END
    FROM bronze.Product;
END
GO
-- LEFT OUTER JOIN
CREATE PROCEDURE [dbo].[usp_LoadWithLeftJoin]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(p.ProductID AS NVARCHAR(25)), COALESCE(c.ConfigValue, p.ProductName)
    FROM bronze.Product p
    LEFT OUTER JOIN dbo.Config c ON c.ConfigKey = CAST(p.ProductID AS NVARCHAR(100));
END
GO
-- IF/ELSE with dual merge
CREATE PROCEDURE [dbo].[usp_ConditionalMerge]
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM dbo.Config WHERE ConfigKey = 'full_reload')
    BEGIN
        TRUNCATE TABLE [silver].[DimProduct];
        INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
        SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product;
    END
    ELSE
    BEGIN
        MERGE INTO [silver].[DimProduct] AS tgt
        USING bronze.Product AS src ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))
        WHEN MATCHED THEN
            UPDATE SET tgt.EnglishProductName = src.ProductName
        WHEN NOT MATCHED THEN
            INSERT (ProductAlternateKey, EnglishProductName)
            VALUES (CAST(src.ProductID AS NVARCHAR(25)), src.ProductName);
    END
END
GO
-- BEGIN TRY / CATCH
CREATE PROCEDURE [dbo].[usp_TryCatchLoad]
AS
BEGIN
    BEGIN TRY
        INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
        SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.Config (ConfigKey, ConfigValue)
        SELECT 'error', ERROR_MESSAGE();
    END CATCH
END
GO
-- Correlated subquery
CREATE PROCEDURE [dbo].[usp_CorrelatedSubquery]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(p.ProductID AS NVARCHAR(25)), p.ProductName
    FROM bronze.Product p
    WHERE p.ProductID = (
        SELECT MAX(p2.ProductID) FROM bronze.Product p2
        WHERE p2.ProductName = p.ProductName
    );
END
GO
-- Sequential WITH blocks: second WITH reads table populated by first WITH
CREATE PROCEDURE [dbo].[usp_SequentialWith]
AS
BEGIN
    SET NOCOUNT ON;
    WITH base_products AS (
        SELECT ProductID, ProductName
        FROM bronze.Product
        WHERE ProductName IS NOT NULL
    )
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM base_products;

    WITH enriched AS (
        SELECT p.ProductAlternateKey, p.EnglishProductName, c.ConfigValue AS Category
        FROM [silver].[DimProduct] p
        JOIN dbo.Config c ON c.ConfigKey = 'default_category'
    ),
    ranked AS (
        SELECT ProductAlternateKey, EnglishProductName, Category,
               ROW_NUMBER() OVER (ORDER BY ProductAlternateKey) AS rn
        FROM enriched
    )
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT 'product_' + ProductAlternateKey, EnglishProductName
    FROM ranked WHERE rn <= 5;
END
GO
-- UPDATE with JOIN
CREATE PROCEDURE [dbo].[usp_SimpleUpdate]
AS
BEGIN
    UPDATE [silver].[DimProduct]
    SET EnglishProductName = p.ProductName
    FROM [silver].[DimProduct] d
    JOIN bronze.Product p ON d.ProductAlternateKey = CAST(p.ProductID AS NVARCHAR(25));
END
GO
-- DELETE with WHERE
CREATE PROCEDURE [dbo].[usp_SimpleDelete]
AS
BEGIN
    DELETE FROM [silver].[DimProduct]
    WHERE ProductAlternateKey IS NULL;
END
GO
-- DELETE TOP
CREATE PROCEDURE [dbo].[usp_DeleteTop]
AS
BEGIN
    DELETE TOP (500) FROM [silver].[DimProduct]
    WHERE ProductKey < 0;
END
GO
-- TRUNCATE only
CREATE PROCEDURE [dbo].[usp_TruncateOnly]
AS
BEGIN
    TRUNCATE TABLE [silver].[DimProduct];
END
GO
-- SELECT INTO
CREATE PROCEDURE [dbo].[usp_SelectInto]
AS
BEGIN
    SELECT ProductID, ProductName
    INTO silver.DimProduct_Staging
    FROM bronze.Product
    WHERE ProductName IS NOT NULL;
END
GO
-- RIGHT OUTER JOIN
CREATE PROCEDURE [dbo].[usp_RightOuterJoin]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(p.ProductID AS NVARCHAR(25)), COALESCE(c.ConfigValue, p.ProductName)
    FROM dbo.Config c
    RIGHT OUTER JOIN bronze.Product p ON c.ConfigKey = CAST(p.ProductID AS NVARCHAR(100));
END
GO
-- Subquery in WHERE
CREATE PROCEDURE [dbo].[usp_SubqueryInWhere]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName
    FROM bronze.Product
    WHERE ProductID > (SELECT AVG(ProductID) FROM bronze.Product);
END
GO
-- Window function
CREATE PROCEDURE [dbo].[usp_WindowFunction]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName
    FROM (
        SELECT ProductID, ProductName,
               ROW_NUMBER() OVER (ORDER BY ProductID DESC) AS rn,
               COUNT(*) OVER () AS total
        FROM bronze.Product
    ) ranked
    WHERE rn <= 10;
END
GO
-- WHILE loop with DML
CREATE PROCEDURE [dbo].[usp_WhileLoop]
AS
BEGIN
    SET NOCOUNT ON;
    WHILE EXISTS (SELECT 1 FROM bronze.Product WHERE ProductName IS NULL)
    BEGIN
        DELETE TOP (100) FROM bronze.Product WHERE ProductName IS NULL;
        INSERT INTO dbo.Config (ConfigKey, ConfigValue) SELECT 'cleanup_run', GETDATE();
    END
END
GO
-- Nested control flow: IF inside TRY/CATCH
CREATE PROCEDURE [dbo].[usp_NestedControlFlow]
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        IF EXISTS (SELECT 1 FROM dbo.Config WHERE ConfigKey = 'full_reload')
        BEGIN
            TRUNCATE TABLE [silver].[DimProduct];
            INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
            SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product;
        END
        ELSE
        BEGIN
            MERGE INTO [silver].[DimProduct] AS tgt
            USING bronze.Product AS src ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))
            WHEN MATCHED THEN UPDATE SET tgt.EnglishProductName = src.ProductName
            WHEN NOT MATCHED THEN INSERT (ProductAlternateKey, EnglishProductName)
                VALUES (CAST(src.ProductID AS NVARCHAR(25)), src.ProductName);
        END
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.Config (ConfigKey, ConfigValue) SELECT 'error', ERROR_MESSAGE();
    END CATCH
END
GO
-- EXEC simple proc call
CREATE PROCEDURE [dbo].[usp_ExecSimple]
AS
BEGIN
    EXEC dbo.usp_LoadDimProduct;
END
GO
-- EXEC bracketed
CREATE PROCEDURE [dbo].[usp_ExecBracketed]
AS
BEGIN
    EXEC [dbo].[usp_LoadDimProduct];
END
GO
-- EXEC with params
CREATE PROCEDURE [dbo].[usp_ExecWithParams]
AS
BEGIN
    EXEC dbo.usp_LoadDimProduct @Mode = 1, @Force = 0;
END
GO
-- EXEC with return value
CREATE PROCEDURE [dbo].[usp_ExecWithReturn]
AS
BEGIN
    DECLARE @rc INT;
    EXEC @rc = dbo.usp_LoadDimProduct;
END
GO
-- EXEC dynamic SQL
CREATE PROCEDURE [dbo].[usp_ExecDynamic]
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO [silver].[DimProduct] (ProductAlternateKey) SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product';
    EXEC (@sql);
END
GO
-- EXEC sp_executesql
CREATE PROCEDURE [dbo].[usp_ExecSpExecutesql]
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO [silver].[DimProduct] (ProductAlternateKey) SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product WHERE ProductID > @minId';
    EXEC sp_executesql @sql, N'@minId INT', @minId = 100;
END
GO
-- Proc that mentions 'silver.DimProduct' only in a string literal / comment
-- This proc must NOT appear in refs results for silver.dimproduct (no real reference)
CREATE PROCEDURE [dbo].[usp_LogMessage]
AS
BEGIN
    -- This proc does NOT reference silver.DimProduct in any DML
    -- PRINT 'silver.DimProduct' is just a string literal
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT 'last_run', CONVERT(NVARCHAR(50), GETDATE(), 120)
END
GO
-- Proc reading from a view (for transitive dependency resolution testing)
CREATE PROCEDURE [dbo].[usp_LoadFromView]
AS BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName)
    SELECT ProductAlternateKey, EnglishProductName
    FROM silver.vw_ProductCatalog
END
GO
-- Proc using a scalar function (for transitive dependency resolution testing)
CREATE PROCEDURE [dbo].[usp_LoadWithFunction]
AS BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName)
    SELECT ProductAlternateKey, dbo.fn_GetRegion(GeographyId)
    FROM bronze.Product
END
GO
