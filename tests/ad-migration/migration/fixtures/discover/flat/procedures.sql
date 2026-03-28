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
