CREATE PROCEDURE dbo.usp_string_only AS
DECLARE @msg NVARCHAR(200) = N'INSERT INTO silver.FactSales is not allowed';
SELECT @msg;
GO
