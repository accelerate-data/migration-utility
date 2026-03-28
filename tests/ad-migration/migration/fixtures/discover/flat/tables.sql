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
CREATE TABLE bronze.Customer (
    CustomerKey BIGINT NOT NULL PRIMARY KEY,
    FirstName NVARCHAR(50) NULL
)
GO
CREATE TABLE bronze.Sales (
    SalesKey BIGINT NOT NULL PRIMARY KEY,
    CustomerKey BIGINT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL
)
GO
CREATE TABLE bronze.SalesOrder (
    OrderID BIGINT NOT NULL PRIMARY KEY,
    CustomerID BIGINT NOT NULL,
    TotalDue DECIMAL(18,2) NOT NULL,
    OrderDate DATE NOT NULL
)
GO
CREATE TABLE bronze.Geography (
    CustomerKey BIGINT NOT NULL PRIMARY KEY,
    Country NVARCHAR(50) NULL
)
GO
CREATE TABLE bronze.RunControl (
    IsActive BIT NOT NULL
)
GO
CREATE TABLE dbo.Config (
    ConfigKey NVARCHAR(100) NOT NULL PRIMARY KEY,
    ConfigValue NVARCHAR(500) NULL
)
GO
