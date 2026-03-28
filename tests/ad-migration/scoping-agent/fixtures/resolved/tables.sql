CREATE TABLE [silver].[DimProduct]
(
    [ProductKey] INT NOT NULL,
    [ProductName] NVARCHAR(200) NOT NULL,
    [Color] NVARCHAR(50) NULL,
    CONSTRAINT [PK_silver_DimProduct] PRIMARY KEY ([ProductKey])
)
GO
