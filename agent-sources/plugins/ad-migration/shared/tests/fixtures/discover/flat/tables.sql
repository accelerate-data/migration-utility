CREATE TABLE [silver].[DimProduct] (
    ProductKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductAlternateKey NVARCHAR(25) NOT NULL,
    EnglishProductName NVARCHAR(50) NOT NULL
)
GO
CREATE TABLE bronze.Product (
    ProductID INT NOT NULL PRIMARY KEY,
    ProductName NVARCHAR(50) NOT NULL
)
GO
CREATE TABLE dbo.Config (
    ConfigKey NVARCHAR(100) NOT NULL PRIMARY KEY,
    ConfigValue NVARCHAR(500) NULL
)
GO
