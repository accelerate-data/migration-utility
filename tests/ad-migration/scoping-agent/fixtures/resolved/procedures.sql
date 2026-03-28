CREATE PROCEDURE [silver].[usp_load_DimProduct]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE [silver].[DimProduct];

    INSERT INTO [silver].[DimProduct] ([ProductKey], [ProductName], [Color])
    SELECT
        p.[ProductKey],
        p.[EnglishProductName],
        p.[Color]
    FROM [bronze].[DimProduct] AS p;
END
GO
