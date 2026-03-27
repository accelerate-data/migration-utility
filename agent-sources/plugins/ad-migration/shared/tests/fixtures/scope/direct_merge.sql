CREATE PROCEDURE [dbo].[usp_direct_merge]
AS BEGIN
    MERGE INTO silver.FactSales AS tgt
    USING (SELECT SalesKey, Amount FROM bronze.SalesRaw) AS src
    ON tgt.SalesKey = src.SalesKey
    WHEN MATCHED THEN UPDATE SET tgt.Amount = src.Amount
    WHEN NOT MATCHED THEN INSERT (SalesKey, Amount) VALUES (src.SalesKey, src.Amount);
END
GO
