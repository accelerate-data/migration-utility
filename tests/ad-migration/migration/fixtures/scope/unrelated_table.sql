CREATE PROCEDURE dbo.usp_writes_other AS
INSERT INTO silver.DimProduct (ProductKey) SELECT ProductKey FROM bronze.ProductRaw;
GO
