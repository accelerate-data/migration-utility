-- Target is a view — the scoping agent is asked to scope a view, not a base table
CREATE VIEW [silver].[vw_DimCurrency]
AS
    SELECT c.[CurrencyKey], c.[CurrencyCode], c.[CurrencyName]
    FROM [silver].[DimCurrency] AS c
    WHERE c.[IsActive] = 1
GO
