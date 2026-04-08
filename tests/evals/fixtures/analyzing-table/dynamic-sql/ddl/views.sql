-- ============================================================
-- SCENARIO: view over a table loaded via dynamic SQL
-- ============================================================
CREATE VIEW silver.vw_DimCurrency
AS
SELECT
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM silver.DimCurrency;

GO
