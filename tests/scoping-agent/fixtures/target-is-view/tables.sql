-- Base table exists but is NOT the item being scoped — the target is the view vw_DimCurrency
CREATE TABLE [silver].[DimCurrency]
(
    [CurrencyKey] INT NOT NULL,
    [CurrencyCode] NVARCHAR(3) NOT NULL,
    [CurrencyName] NVARCHAR(100) NOT NULL,
    [IsActive] BIT NOT NULL,
    CONSTRAINT [PK_silver_DimCurrency] PRIMARY KEY ([CurrencyKey])
)
GO
