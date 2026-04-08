-- ============================================================
-- SCENARIO: GROUPING SETS — subtotal plus grand total
-- ============================================================
CREATE PROCEDURE silver.usp_load_GroupingSetsTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.GroupingSetsTarget;
    INSERT INTO silver.GroupingSetsTarget (GroupKey, ProductCount)
    SELECT
        COALESCE(Color, 'all'),
        COUNT(*)
    FROM bronze.Product
    GROUP BY GROUPING SETS ((Color), ());
END;

GO
