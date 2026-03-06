CREATE TABLE [silver].[DimRegion]
(
    [RegionKey] INT NOT NULL,
    [RegionCode] NVARCHAR(10) NOT NULL,
    [RegionName] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_silver_DimRegion] PRIMARY KEY ([RegionKey])
)
GO
