-- ============================================================
-- SCENARIO: partial (dynamic SQL only — no static write visible)
-- ============================================================
CREATE PROCEDURE MigrationTest.silver_usp_load_dimcurrency
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql NVARCHAR(MAX);
    -- Truncate first
    TRUNCATE TABLE MigrationTest.silver_dimcurrency;
    -- All writes via dynamic SQL so scoping agent sees no static INSERT/MERGE
    SET @sql = N'
        INSERT INTO MigrationTest.silver_dimcurrency (CurrencyAlternateKey, CurrencyName)
        SELECT CurrencyCode, CurrencyName
        FROM MigrationTest.bronze_currency';
    EXEC sp_executesql @sql;
END;

GO
