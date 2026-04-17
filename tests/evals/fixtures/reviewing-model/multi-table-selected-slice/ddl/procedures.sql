-- ============================================================
-- SCENARIO: multi-table writer selected slice
-- The target-table slice is SelectIntoTarget. UnrelatedTarget logic must
-- not influence model review for SelectIntoTarget.
-- ============================================================
CREATE PROCEDURE silver.usp_load_SelectIntoTarget
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE silver.SelectIntoTarget;
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName
    INTO silver.SelectIntoTarget
    FROM bronze.Product;

    TRUNCATE TABLE silver.UnrelatedTarget;
    SELECT
        CAST(CreditCardID AS NVARCHAR(25)) AS CreditCardAlternateKey,
        CardType AS CreditCardName
    INTO silver.UnrelatedTarget
    FROM bronze.CreditCard;
END;

GO
