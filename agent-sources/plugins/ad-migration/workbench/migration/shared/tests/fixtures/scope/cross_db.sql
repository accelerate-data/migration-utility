CREATE PROCEDURE [dbo].[usp_cross_db]
AS BEGIN
    INSERT INTO [OtherServer].[OtherDB].[dbo].[FactSales] (SalesKey)
    SELECT SalesKey FROM bronze.SalesRaw
END
GO
