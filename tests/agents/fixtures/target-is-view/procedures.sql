-- Procedures only write to the base table, not the view — view has no writer proc
CREATE PROCEDURE [silver].[usp_load_DimCurrency]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE [silver].[DimCurrency];

    INSERT INTO [silver].[DimCurrency] ([CurrencyKey], [CurrencyCode], [CurrencyName], [IsActive])
    SELECT
        c.[CurrencyID],
        c.[ISO3],
        c.[CurrencyName],
        c.[IsActive]
    FROM [bronze].[Currency] AS c;
END
GO
