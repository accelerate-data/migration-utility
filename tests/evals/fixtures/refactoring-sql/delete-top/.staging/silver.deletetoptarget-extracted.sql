SELECT ProductKey, ProductAlternateKey, EnglishProductName, LoadedDate
FROM silver.DeleteTopTarget
WHERE LoadedDate >= DATEADD(YEAR, -1, GETDATE())
OR LoadedDate >= (
  SELECT ISNULL(MIN(LoadedDate), DATEADD(YEAR, -1, GETDATE()))
  FROM (
    SELECT TOP (100) LoadedDate
    FROM silver.DeleteTopTarget
    WHERE LoadedDate < DATEADD(YEAR, -1, GETDATE())
    ORDER BY LoadedDate ASC
  ) AS TopRows
)
ORDER BY LoadedDate ASC;