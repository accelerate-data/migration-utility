CREATE TABLE [bronze].[CountryRegion] (
    [CountryRegionCode] nvarchar(3) NOT NULL,
    [CountryRegionName] nvarchar(50) NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Currency] (
    [CurrencyCode] nchar(3) NOT NULL,
    [CurrencyName] nvarchar(50) NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Customer] (
    [CustomerID] int IDENTITY(1,1) NOT NULL,
    [PersonID] int NULL,
    [StoreID] int NULL,
    [TerritoryID] int NULL,
    [AccountNumber] varchar(10) NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Employee] (
    [BusinessEntityID] int NOT NULL,
    [NationalIDNumber] nvarchar(15) NOT NULL,
    [LoginID] nvarchar(256) NOT NULL,
    [JobTitle] nvarchar(50) NOT NULL,
    [BirthDate] date NOT NULL,
    [MaritalStatus] nchar(1) NOT NULL,
    [Gender] nchar(1) NOT NULL,
    [HireDate] date NOT NULL,
    [SalariedFlag] bit NOT NULL,
    [VacationHours] smallint NOT NULL,
    [SickLeaveHours] smallint NOT NULL,
    [CurrentFlag] bit NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Person] (
    [BusinessEntityID] int NOT NULL,
    [PersonType] nchar(2) NOT NULL,
    [Title] nvarchar(8) NULL,
    [FirstName] nvarchar(50) NOT NULL,
    [MiddleName] nvarchar(50) NULL,
    [LastName] nvarchar(50) NOT NULL,
    [Suffix] nvarchar(10) NULL,
    [EmailPromotion] int NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Product] (
    [ProductID] int IDENTITY(1,1) NOT NULL,
    [ProductName] nvarchar(50) NOT NULL,
    [ProductNumber] nvarchar(25) NOT NULL,
    [MakeFlag] bit NOT NULL,
    [FinishedGoodsFlag] bit NOT NULL,
    [Color] nvarchar(15) NULL,
    [SafetyStockLevel] smallint NOT NULL,
    [ReorderPoint] smallint NOT NULL,
    [StandardCost] money NOT NULL,
    [ListPrice] money NOT NULL,
    [Size] nvarchar(5) NULL,
    [SizeUnitMeasureCode] nchar(3) NULL,
    [WeightUnitMeasureCode] nchar(3) NULL,
    [Weight] float NULL,
    [DaysToManufacture] int NOT NULL,
    [ProductLine] nchar(2) NULL,
    [Class] nchar(2) NULL,
    [Style] nchar(2) NULL,
    [ProductSubcategoryID] int NULL,
    [ProductModelID] int NULL,
    [SellStartDate] datetime NOT NULL,
    [SellEndDate] datetime NULL,
    [DiscontinuedDate] datetime NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[Promotion] (
    [PromotionID] int IDENTITY(1,1) NOT NULL,
    [Description] nvarchar(255) NOT NULL,
    [DiscountPct] float NULL,
    [PromotionType] nvarchar(50) NOT NULL,
    [PromotionCategory] nvarchar(50) NOT NULL,
    [StartDate] datetime NOT NULL,
    [EndDate] datetime NOT NULL,
    [MinQty] int NOT NULL,
    [MaxQty] int NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[SalesOrderDetail] (
    [SalesOrderID] int NOT NULL,
    [SalesOrderDetailID] int IDENTITY(1,1) NOT NULL,
    [CarrierTrackingNumber] nvarchar(25) NULL,
    [OrderQty] smallint NOT NULL,
    [ProductID] int NOT NULL,
    [SpecialOfferID] int NOT NULL,
    [UnitPrice] money NOT NULL,
    [UnitPriceDiscount] money NOT NULL,
    [LineTotal] money NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[SalesOrderHeader] (
    [SalesOrderID] int IDENTITY(1,1) NOT NULL,
    [RevisionNumber] tinyint NOT NULL,
    [OrderDate] datetime NOT NULL,
    [DueDate] datetime NOT NULL,
    [ShipDate] datetime NULL,
    [Status] tinyint NOT NULL,
    [OnlineOrderFlag] bit NOT NULL,
    [SalesOrderNumber] nvarchar(25) NOT NULL,
    [PurchaseOrderNumber] nvarchar(25) NULL,
    [AccountNumber] nvarchar(15) NULL,
    [CustomerID] int NOT NULL,
    [SalesPersonID] int NULL,
    [TerritoryID] int NULL,
    [BillToAddressID] int NOT NULL,
    [ShipToAddressID] int NOT NULL,
    [ShipMethodID] int NOT NULL,
    [CreditCardID] int NULL,
    [SubTotal] money NOT NULL,
    [TaxAmt] money NOT NULL,
    [Freight] money NOT NULL,
    [TotalDue] money NOT NULL,
    [Comment] nvarchar(128) NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[SalesTerritory] (
    [TerritoryID] int IDENTITY(1,1) NOT NULL,
    [TerritoryName] nvarchar(50) NOT NULL,
    [CountryRegionCode] nvarchar(3) NOT NULL,
    [TerritoryGroup] nvarchar(50) NOT NULL,
    [SalesYTD] money NOT NULL,
    [SalesLastYear] money NOT NULL,
    [CostYTD] money NOT NULL,
    [CostLastYear] money NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [bronze].[StateProvince] (
    [StateProvinceID] int IDENTITY(1,1) NOT NULL,
    [StateProvinceCode] nchar(3) NOT NULL,
    [CountryRegionCode] nvarchar(3) NOT NULL,
    [IsOnlyStateProvinceFlag] bit NOT NULL,
    [StateProvinceName] nvarchar(50) NOT NULL,
    [TerritoryID] int NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO
 
CREATE TABLE [silver].[DimCrossDbProfile] (
    [CrossDbProfileKey] int IDENTITY(1,1) NOT NULL,
    [ProfileCode] nvarchar(20) NOT NULL,
    [ProfileName] nvarchar(100) NOT NULL,
    [ProfileCategory] nvarchar(50) NULL,
    [ActiveFlag] bit NOT NULL,
    [LoadedAt] datetime NULL
    ,CONSTRAINT [PK_DimCrossDbProfile] PRIMARY KEY ([CrossDbProfileKey])
);
GO

CREATE TABLE [silver].[DimCurrency] (
    [CurrencyKey] int IDENTITY(1,1) NOT NULL,
    [CurrencyAlternateKey] nchar(3) NOT NULL,
    [CurrencyName] nvarchar(50) NOT NULL
    ,CONSTRAINT [PK_DimCurrency] PRIMARY KEY ([CurrencyKey])
);
GO
 
CREATE TABLE [silver].[DimCustomer] (
    [CustomerKey] int IDENTITY(1,1) NOT NULL,
    [CustomerAlternateKey] nvarchar(15) NOT NULL,
    [FirstName] nvarchar(50) NULL,
    [MiddleName] nvarchar(50) NULL,
    [LastName] nvarchar(50) NULL,
    [Title] nvarchar(8) NULL,
    [Gender] nvarchar(1) NULL,
    [MaritalStatus] nchar(1) NULL,
    [EmailPromotion] int NULL,
    [DateFirstPurchase] date NULL
    ,CONSTRAINT [PK_DimCustomer] PRIMARY KEY ([CustomerKey])
);
GO
 
CREATE TABLE [silver].[DimEmployee] (
    [EmployeeKey] int IDENTITY(1,1) NOT NULL,
    [EmployeeAlternateKey] nvarchar(15) NULL,
    [FirstName] nvarchar(50) NOT NULL,
    [LastName] nvarchar(50) NOT NULL,
    [Title] nvarchar(50) NULL,
    [HireDate] date NULL,
    [Gender] nchar(1) NULL,
    [MaritalStatus] nchar(1) NULL,
    [SalariedFlag] bit NULL,
    [CurrentFlag] bit NOT NULL
    ,CONSTRAINT [PK_DimEmployee] PRIMARY KEY ([EmployeeKey])
);
GO
 
CREATE TABLE [silver].[DimGeography] (
    [GeographyKey] int IDENTITY(1,1) NOT NULL,
    [StateProvinceCode] nvarchar(3) NULL,
    [StateProvinceName] nvarchar(50) NULL,
    [CountryRegionCode] nvarchar(3) NULL,
    [CountryRegionName] nvarchar(50) NULL
    ,CONSTRAINT [PK_DimGeography] PRIMARY KEY ([GeographyKey])
);
GO
 
CREATE TABLE [silver].[DimProduct] (
    [ProductKey] int IDENTITY(1,1) NOT NULL,
    [ProductAlternateKey] nvarchar(25) NULL,
    [EnglishProductName] nvarchar(50) NOT NULL,
    [StandardCost] money NULL,
    [ListPrice] money NULL,
    [Color] nvarchar(15) NOT NULL,
    [Size] nvarchar(50) NULL,
    [ProductLine] nchar(2) NULL,
    [Class] nchar(2) NULL,
    [Style] nchar(2) NULL,
    [StartDate] datetime NULL,
    [EndDate] datetime NULL,
    [Status] nvarchar(10) NULL
    ,CONSTRAINT [PK_DimProduct] PRIMARY KEY ([ProductKey])
);
GO
 
CREATE TABLE [silver].[DimPromotion] (
    [PromotionKey] int IDENTITY(1,1) NOT NULL,
    [PromotionAlternateKey] int NULL,
    [EnglishPromotionName] nvarchar(255) NULL,
    [DiscountPct] float NULL,
    [EnglishPromotionType] nvarchar(50) NULL,
    [EnglishPromotionCategory] nvarchar(50) NULL,
    [StartDate] datetime NOT NULL,
    [EndDate] datetime NULL,
    [MinQty] int NULL,
    [MaxQty] int NULL
    ,CONSTRAINT [PK_DimPromotion] PRIMARY KEY ([PromotionKey])
);
GO
 
CREATE TABLE [silver].[DimSalesTerritory] (
    [SalesTerritoryKey] int IDENTITY(1,1) NOT NULL,
    [SalesTerritoryAlternateKey] int NULL,
    [SalesTerritoryRegion] nvarchar(50) NOT NULL,
    [SalesTerritoryCountry] nvarchar(50) NOT NULL,
    [SalesTerritoryGroup] nvarchar(50) NULL
    ,CONSTRAINT [PK_DimSalesTerritory] PRIMARY KEY ([SalesTerritoryKey])
);
GO
 
CREATE TABLE [silver].[FactInternetSales] (
    [SalesOrderNumber] nvarchar(20) NOT NULL,
    [SalesOrderLineNumber] tinyint NOT NULL,
    [ProductKey] int NOT NULL,
    [CustomerKey] int NOT NULL,
    [SalesTerritoryKey] int NULL,
    [OrderQuantity] smallint NOT NULL,
    [UnitPrice] money NOT NULL,
    [ExtendedAmount] money NULL,
    [SalesAmount] money NOT NULL,
    [TaxAmt] money NULL,
    [Freight] money NULL,
    [OrderDate] datetime NULL,
    [DueDate] datetime NULL,
    [ShipDate] datetime NULL
    ,CONSTRAINT [PK_FactInternetSales] PRIMARY KEY ([SalesOrderNumber], [SalesOrderLineNumber])
);
GO
 

CREATE TABLE [silver].[IfElseTarget] (
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL,
    [PriceCategory] nvarchar(20) NOT NULL
    ,CONSTRAINT [PK_IfElseTarget] PRIMARY KEY ([ProductAlternateKey])
);
GO

CREATE TABLE [silver].[WhileTarget] (
    [BatchId] int NOT NULL,
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL
    ,CONSTRAINT [PK_WhileTarget] PRIMARY KEY ([BatchId], [ProductAlternateKey])
);
GO

CREATE TABLE [silver].[StaticSpExecTarget] (
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL
    ,CONSTRAINT [PK_StaticSpExecTarget] PRIMARY KEY ([ProductAlternateKey])
);
GO

CREATE TABLE [silver].[FactExecProfile] (
    [ExecProfileKey] int IDENTITY(1,1) NOT NULL,
    [ProcedureKey] int NOT NULL,
    [ExecutionDate] datetime NOT NULL,
    [DurationMs] bigint NOT NULL,
    [RowsAffected] int NOT NULL,
    [StatusCode] tinyint NOT NULL
    ,CONSTRAINT [PK_FactExecProfile] PRIMARY KEY ([ExecProfileKey])
);
GO

CREATE TABLE [silver].[IfElseTarget] (
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL,
    [ModifiedDate] datetime NOT NULL
);
GO

CREATE TABLE [silver].[WhileLoopTarget] (
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL
);
GO

CREATE TABLE [silver].[StaticSpExecTarget] (
    [ProductAlternateKey] nvarchar(25) NOT NULL,
    [EnglishProductName] nvarchar(50) NOT NULL
);
GO

-- silver.DimDate (lookup reference for role-playing FK tests)
CREATE TABLE [silver].[DimDate] (
    [DateKey] int NOT NULL,
    [FullDate] date NOT NULL,
    [DayOfWeek] tinyint NOT NULL,
    [MonthNumber] tinyint NOT NULL,
    [Quarter] tinyint NOT NULL,
    [Year] smallint NOT NULL,
    [MonthName] nvarchar(20) NOT NULL,
    [DayName] nvarchar(20) NOT NULL
    ,CONSTRAINT [PK_DimDate] PRIMARY KEY ([DateKey])
);
GO

-- silver.DimEmployeeSCD2 (SCD Type 2 dimension)
CREATE TABLE [silver].[DimEmployeeSCD2] (
    [EmployeeSCD2Key] int IDENTITY(1,1) NOT NULL,
    [EmployeeNaturalKey] nvarchar(15) NOT NULL,
    [FirstName] nvarchar(50) NOT NULL,
    [LastName] nvarchar(50) NOT NULL,
    [JobTitle] nvarchar(50) NOT NULL,
    [Department] nvarchar(50) NULL,
    [ValidFrom] datetime2 NOT NULL,
    [ValidTo] datetime2 NOT NULL,
    [IsCurrent] bit NOT NULL
    ,CONSTRAINT [PK_DimEmployeeSCD2] PRIMARY KEY ([EmployeeSCD2Key])
);
GO

-- silver.FactInventorySnapshot (periodic snapshot)
CREATE TABLE [silver].[FactInventorySnapshot] (
    [ProductKey] int NOT NULL,
    [WarehouseKey] int NOT NULL,
    [SnapshotDate] date NOT NULL,
    [UnitsOnHand] int NOT NULL,
    [UnitsOnOrder] int NOT NULL,
    [ReorderPoint] int NOT NULL,
    [UnitCost] money NOT NULL
    ,CONSTRAINT [PK_FactInventorySnapshot] PRIMARY KEY ([ProductKey], [WarehouseKey], [SnapshotDate])
);
GO

-- silver.DimContact (PII-rich dimension)
CREATE TABLE [silver].[DimContact] (
    [ContactKey] int IDENTITY(1,1) NOT NULL,
    [ContactAlternateKey] nvarchar(15) NOT NULL,
    [FirstName] nvarchar(50) NOT NULL,
    [LastName] nvarchar(50) NOT NULL,
    [EmailAddress] nvarchar(100) NULL,
    [PhoneNumber] nvarchar(25) NULL,
    [SocialSecurityNumber] nvarchar(11) NULL,
    [BirthDate] date NULL,
    [StreetAddress] nvarchar(200) NULL,
    [City] nvarchar(50) NULL,
    [PostalCode] nvarchar(15) NULL
    ,CONSTRAINT [PK_DimContact] PRIMARY KEY ([ContactKey])
);
GO

-- silver.FactOrderFulfillment (accumulating snapshot)
CREATE TABLE [silver].[FactOrderFulfillment] (
    [OrderFulfillmentKey] int IDENTITY(1,1) NOT NULL,
    [SalesOrderNumber] nvarchar(25) NOT NULL,
    [CustomerKey] int NOT NULL,
    [ProductKey] int NOT NULL,
    [OrderDate] datetime NOT NULL,
    [ShipDate] datetime NULL,
    [DeliveryDate] datetime NULL,
    [InvoiceDate] datetime NULL,
    [OrderAmount] money NOT NULL,
    [ShippingCost] money NULL
    ,CONSTRAINT [PK_FactOrderFulfillment] PRIMARY KEY ([OrderFulfillmentKey])
);
GO

-- silver.FactResellerSales (role-playing FK fact)
CREATE TABLE [silver].[FactResellerSales] (
    [ResellerSalesKey] int IDENTITY(1,1) NOT NULL,
    [ProductKey] int NOT NULL,
    [OrderDateKey] int NOT NULL,
    [ShipDateKey] int NOT NULL,
    [DueDateKey] int NOT NULL,
    [CustomerKey] int NOT NULL,
    [OrderQuantity] smallint NOT NULL,
    [UnitPrice] money NOT NULL,
    [SalesAmount] money NOT NULL,
    [TaxAmt] money NOT NULL,
    [Freight] money NOT NULL
    ,CONSTRAINT [PK_FactResellerSales] PRIMARY KEY ([ResellerSalesKey])
);
GO

-- silver.FactProductSalesDelta (incremental load with WHERE watermark)
CREATE TABLE [silver].[FactProductSalesDelta] (
    [SalesDeltaKey] int IDENTITY(1,1) NOT NULL,
    [ProductKey] int NOT NULL,
    [CustomerKey] int NOT NULL,
    [SalesAmount] money NOT NULL,
    [OrderDate] datetime NOT NULL,
    [ModifiedDate] datetime NOT NULL
    ,CONSTRAINT [PK_FactProductSalesDelta] PRIMARY KEY ([SalesDeltaKey])
);
GO

-- silver.DimSalesFlags (junk dimension)
CREATE TABLE [silver].[DimSalesFlags] (
    [SalesFlagKey] int IDENTITY(1,1) NOT NULL,
    [IsOnlineOrder] bit NOT NULL,
    [IsRushShipment] bit NOT NULL,
    [IsGiftWrapped] bit NOT NULL,
    [IsDiscounted] bit NOT NULL,
    [IsReturnable] bit NOT NULL
    ,CONSTRAINT [PK_DimSalesFlags] PRIMARY KEY ([SalesFlagKey])
);
GO

-- silver.DimMultiWriter (3+ writer disambiguation test)
CREATE TABLE [silver].[DimMultiWriter] (
    [MultiWriterKey] int IDENTITY(1,1) NOT NULL,
    [AlternateKey] nvarchar(20) NOT NULL,
    [DisplayName] nvarchar(100) NOT NULL,
    [Category] nvarchar(50) NULL,
    [IsActive] bit NOT NULL,
    [ModifiedDate] datetime NOT NULL
    ,CONSTRAINT [PK_DimMultiWriter] PRIMARY KEY ([MultiWriterKey])
);
GO

-- silver.DimDynamicBranch (IF/ELSE dynamic SQL test)
CREATE TABLE [silver].[DimDynamicBranch] (
    [DynamicBranchKey] int IDENTITY(1,1) NOT NULL,
    [BranchCode] nvarchar(20) NOT NULL,
    [BranchName] nvarchar(100) NOT NULL,
    [Region] nvarchar(50) NULL,
    [LoadedAt] datetime NOT NULL
    ,CONSTRAINT [PK_DimDynamicBranch] PRIMARY KEY ([DynamicBranchKey])
);
GO
