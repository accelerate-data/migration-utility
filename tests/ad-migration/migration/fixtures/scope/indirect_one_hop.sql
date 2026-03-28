CREATE PROCEDURE dbo.usp_writer
AS BEGIN
    INSERT INTO silver.FactSales (SalesKey) SELECT SalesKey FROM bronze.SalesRaw
END
GO
CREATE PROCEDURE dbo.usp_caller
AS BEGIN
    EXEC dbo.usp_writer
END
GO
