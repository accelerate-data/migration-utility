CREATE PROCEDURE [dbo].[usp_dynamic_only]
AS BEGIN
    DECLARE @tbl NVARCHAR(200) = N'silver.FactSales'
    DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO ' + @tbl + N' (SalesKey) SELECT SalesKey FROM bronze.SalesRaw'
    EXEC sp_executesql @sql
END
GO
