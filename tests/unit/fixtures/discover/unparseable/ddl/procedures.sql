-- Good proc: loads and parses fine
CREATE PROCEDURE [dbo].[usp_Good]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (ProductAlternateKey)
    SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product;
END
GO
-- Bad proc: completely invalid SQL, stored with parse_error
CREATE PROCEDURE [dbo].[usp_Bad]
AS
BEGIN
    THIS IS NOT VALID SQL AT ALL;
END
GO
