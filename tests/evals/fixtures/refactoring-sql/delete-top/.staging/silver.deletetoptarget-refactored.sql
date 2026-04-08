WITH all_records AS (
    SELECT
        ProductKey,
        ProductAlternateKey,
        EnglishProductName,
        LoadedDate
    FROM silver.DeleteTopTarget
),
old_records_to_delete AS (
    SELECT TOP (100)
        ProductKey,
        ProductAlternateKey,
        EnglishProductName,
        LoadedDate
    FROM all_records
    WHERE LoadedDate < DATEADD(YEAR, -1, GETDATE())
    ORDER BY LoadedDate ASC
),
surviving_records AS (
    SELECT
        ProductKey,
        ProductAlternateKey,
        EnglishProductName,
        LoadedDate
    FROM all_records
    WHERE ProductKey NOT IN (SELECT ProductKey FROM old_records_to_delete)
),
final AS (
    SELECT
        ProductKey,
        ProductAlternateKey,
        EnglishProductName,
        LoadedDate
    FROM surviving_records
)
SELECT * FROM final;