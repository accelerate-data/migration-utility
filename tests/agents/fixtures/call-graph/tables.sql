CREATE TABLE [silver].[DimTerritory]
(
    [TerritoryKey] INT NOT NULL,
    [TerritoryCode] NVARCHAR(10) NOT NULL,
    [TerritoryName] NVARCHAR(100) NOT NULL,
    [RegionGroup] NVARCHAR(50) NOT NULL,
    CONSTRAINT [PK_silver_DimTerritory] PRIMARY KEY ([TerritoryKey])
)
GO
