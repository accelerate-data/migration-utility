CREATE TABLE [silver].[FactSales] (
    sale_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    customer_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    load_date DATETIME2 NOT NULL
)
GO
CREATE TABLE [silver].[DimCustomer] (
    customer_sk BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    customer_name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) NULL,
    valid_from DATETIME2 NOT NULL,
    valid_to DATETIME2 NULL,
    is_current BIT NOT NULL
)
GO
CREATE TABLE [silver].[DimProduct] (
    product_sk BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_name NVARCHAR(100) NOT NULL,
    category NVARCHAR(50) NULL
)
GO
CREATE TABLE [bronze].[Sales] (
    SalesKey BIGINT NOT NULL PRIMARY KEY,
    CustomerKey BIGINT NOT NULL,
    ProductKey BIGINT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    OrderDate DATETIME2 NOT NULL
)
GO
CREATE TABLE [bronze].[Customer] (
    CustomerID BIGINT NOT NULL PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) NULL
)
GO
CREATE TABLE [bronze].[Product] (
    ProductID BIGINT NOT NULL PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50) NULL
)
GO
