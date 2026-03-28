-- Simple INSERT proc: references [silver].[DimProduct] using bracket notation
-- Expected: parseable, refs finds silver.dimproduct via bracket notation
CREATE PROCEDURE [dbo].[usp_LoadDimProduct]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product
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
