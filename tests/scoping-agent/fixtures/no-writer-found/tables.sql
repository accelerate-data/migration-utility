CREATE TABLE [silver].[DimGeography]
(
    [GeographyKey] INT NOT NULL,
    [Country] NVARCHAR(100) NOT NULL,
    [City] NVARCHAR(100) NULL,
    CONSTRAINT [PK_silver_DimGeography] PRIMARY KEY ([GeographyKey])
)
GO
