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


-- ============================================================
-- VIEWS
-- ============================================================

-- Updateable view over silver.DimPromotion (writer-through-view scenario)
CREATE VIEW silver.vw_DimPromotion
WITH SCHEMABINDING
AS
SELECT
    PromotionKey,
    PromotionAlternateKey,
    EnglishPromotionName,
    DiscountPct,
    EnglishPromotionType,
    EnglishPromotionCategory,
    StartDate,
    EndDate,
    MinQty,
    MaxQty
FROM silver.DimPromotion;

GO

