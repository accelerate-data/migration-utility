-- ============================================================
-- SCENARIO: PIVOT — color counts become columns
-- ============================================================
CREATE PROCEDURE silver.usp_load_PivotTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.PivotTarget;
    INSERT INTO silver.PivotTarget (MetricName, RedCount, BlackCount, SilverCount)
    SELECT
        'product_counts',
        ISNULL([Red], 0),
        ISNULL([Black], 0),
        ISNULL([Silver], 0)
    FROM (
        SELECT Color, ProductID
        FROM bronze.Product
    ) AS src
    PIVOT (
        COUNT(ProductID) FOR Color IN ([Red], [Black], [Silver])
    ) AS p;
END;

GO
