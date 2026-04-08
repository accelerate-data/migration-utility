CREATE VIEW silver.vDimSalesTerritory
WITH SCHEMABINDING
AS
SELECT
    SalesTerritoryKey,
    SalesTerritoryAlternateKey,
    SalesTerritoryRegion,
    SalesTerritoryCountry,
    SalesTerritoryGroup
FROM silver.DimSalesTerritory;

GO
