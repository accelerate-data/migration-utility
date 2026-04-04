CREATE TABLE silver.DimCustomer (
    CustomerKey BIGINT NOT NULL,
    FirstName NVARCHAR(50) NULL,
    Region NVARCHAR(50) NULL
);
GO
CREATE TABLE silver.FactSales (
    SalesKey BIGINT NOT NULL,
    CustomerKey BIGINT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL
);
GO
CREATE TABLE bronze.Customer (
    CustomerKey BIGINT NOT NULL,
    FirstName NVARCHAR(50) NULL
);
GO
CREATE TABLE bronze.Sales (
    SalesKey BIGINT NOT NULL,
    CustomerKey BIGINT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL
);
GO
CREATE TABLE bronze.Product (
    ProductID BIGINT NOT NULL,
    Name NVARCHAR(100) NULL,
    ListPrice DECIMAL(18,2) NULL
);
GO
CREATE TABLE bronze.SalesOrder (
    OrderID BIGINT NOT NULL,
    CustomerID BIGINT NOT NULL,
    TotalDue DECIMAL(18,2) NOT NULL,
    OrderDate DATE NOT NULL
);
GO
CREATE TABLE bronze.Geography (
    CustomerKey BIGINT NOT NULL,
    Country NVARCHAR(50) NULL
);
GO
CREATE TABLE bronze.RunControl (
    IsActive BIT NOT NULL
);
GO
