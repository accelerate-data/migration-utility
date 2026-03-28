CREATE PROCEDURE dbo.usp_leaf_writer
AS BEGIN
    INSERT INTO silver.FactSales (SalesKey) SELECT SalesKey FROM bronze.SalesRaw
END
GO
CREATE PROCEDURE dbo.usp_middle
AS BEGIN
    EXEC dbo.usp_leaf_writer
END
GO
CREATE PROCEDURE dbo.usp_outer
AS BEGIN
    EXEC dbo.usp_middle
END
GO
