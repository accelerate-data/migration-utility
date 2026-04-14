-- ============================================================
-- SCENARIO: partial (dynamic SQL only — no static write visible)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCurrency
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql NVARCHAR(MAX);
    -- Truncate first
    TRUNCATE TABLE silver.DimCurrency;
    -- All writes via dynamic SQL so scoping agent sees no static INSERT/MERGE
    SET @sql = N'
        INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
        SELECT CurrencyCode, CurrencyName
        FROM MigrationTest.bronze_currency';
    EXEC sp_executesql @sql;
END;

GO
