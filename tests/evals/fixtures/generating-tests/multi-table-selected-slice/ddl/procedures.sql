-- ============================================================
-- SCENARIO: multi-table writer selected slice
-- The target-table slice is InsertSelectTarget. UnrelatedTarget logic must
-- not influence test generation for InsertSelectTarget.
-- ============================================================
CREATE PROCEDURE silver.usp_load_InsertSelectTarget
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE silver.InsertSelectTarget;
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product;

    TRUNCATE TABLE silver.UnrelatedTarget;
    INSERT INTO silver.UnrelatedTarget (CreditCardAlternateKey, CreditCardName)
    SELECT
        CAST(CreditCardID AS NVARCHAR(25)) AS CreditCardAlternateKey,
        CardType                           AS CreditCardName
    FROM bronze.CreditCard;
END;

GO
