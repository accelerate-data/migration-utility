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
