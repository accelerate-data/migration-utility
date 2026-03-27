CREATE TABLE [silver].[DimProduct] (
    ProductKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductAlternateKey NVARCHAR(25) NOT NULL,
    EnglishProductName NVARCHAR(50) NOT NULL
)
GO
CREATE TABLE [silver].[DimCustomer] (
    CustomerKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerAlternateKey NVARCHAR(15) NOT NULL
)
GO
CREATE TABLE [silver].[FactInternetSales] (
    SalesOrderKey INT NOT NULL,
    ProductKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    CONSTRAINT PK_FactInternetSales PRIMARY KEY (SalesOrderKey)
)
GO
CREATE TABLE [bronze].[Product] (
    ProductID INT NOT NULL PRIMARY KEY,
    ProductName NVARCHAR(50) NOT NULL
)
GO
CREATE TABLE [bronze].[Customer] (
    CustomerID INT NOT NULL PRIMARY KEY,
    AccountNumber NVARCHAR(10) NOT NULL
)
GO
