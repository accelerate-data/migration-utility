CREATE PROCEDURE [dbo].[usp_direct_insert]
AS BEGIN
    INSERT INTO silver.FactSales (SalesKey) SELECT SalesKey FROM bronze.SalesRaw
END
GO
