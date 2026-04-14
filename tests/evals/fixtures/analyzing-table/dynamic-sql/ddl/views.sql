-- ============================================================
-- SCENARIO: view over a table loaded via dynamic SQL
-- ============================================================
CREATE VIEW MigrationTest.silver_vw_dimcurrency
AS
SELECT
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM MigrationTest.silver_dimcurrency;

GO
