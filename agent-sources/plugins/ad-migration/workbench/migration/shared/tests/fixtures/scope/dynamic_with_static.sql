CREATE PROCEDURE [dbo].[usp_dynamic_with_static]
AS BEGIN
    INSERT INTO silver.FactSales (SalesKey) SELECT SalesKey FROM bronze.SalesRaw
    EXEC sp_executesql N'SELECT 1'
END
GO
