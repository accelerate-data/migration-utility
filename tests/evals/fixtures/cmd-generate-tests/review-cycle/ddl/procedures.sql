-- ============================================================
-- SCENARIO: correlated subquery — preserve MAX-per-name filter
-- ============================================================
CREATE PROCEDURE silver.usp_load_CorrelatedSubqueryTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.CorrelatedSubqueryTarget;
    INSERT INTO silver.CorrelatedSubqueryTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(p.ProductID AS NVARCHAR(25)),
        p.ProductName
    FROM bronze.Product AS p
    WHERE p.ProductID = (
        SELECT MAX(p2.ProductID)
        FROM bronze.Product AS p2
        WHERE p2.ProductName = p.ProductName
    );
END;

GO
