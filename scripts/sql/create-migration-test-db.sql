-- ============================================================
-- Section A: DB skeleton + bronze tables
-- Idempotent: drops and recreates MigrationTest
-- ============================================================

USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'MigrationTest')
BEGIN
    ALTER DATABASE MigrationTest SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE MigrationTest;
END
GO

CREATE DATABASE MigrationTest;
GO

USE MigrationTest;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

-- ============================================================
-- Bronze tables — SELECT INTO from AdventureWorks2022
-- (TOP 5000 on large tables to keep test DB small)
-- ============================================================

-- bronze.Product
SELECT
    ProductID, Name AS ProductName, ProductNumber,
    MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint,
    StandardCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode,
    CAST(Weight AS float) AS Weight, DaysToManufacture,
    ProductLine, Class, Style, ProductSubcategoryID, ProductModelID,
    SellStartDate, SellEndDate, DiscontinuedDate, ModifiedDate
INTO bronze.Product
FROM AdventureWorks2022.Production.Product;
GO

-- bronze.Customer (all rows — anchor table for Person and Orders)
SELECT
    CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, ModifiedDate
INTO bronze.Customer
FROM AdventureWorks2022.Sales.Customer;
GO

-- bronze.Person — only persons referenced by bronze.Customer (join-complete)
SELECT
    p.BusinessEntityID, p.PersonType, p.Title, p.FirstName, p.MiddleName, p.LastName,
    p.Suffix, p.EmailPromotion, p.ModifiedDate
INTO bronze.Person
FROM AdventureWorks2022.Person.Person p
WHERE p.BusinessEntityID IN (
    SELECT PersonID FROM bronze.Customer WHERE PersonID IS NOT NULL
);
GO

-- bronze.SalesOrderHeader (all rows)
SELECT
    SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status,
    OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
    CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID,
    ShipMethodID, CreditCardID, SubTotal, TaxAmt, Freight, TotalDue,
    Comment, ModifiedDate
INTO bronze.SalesOrderHeader
FROM AdventureWorks2022.Sales.SalesOrderHeader;
GO

-- bronze.SalesOrderDetail (all rows — no filter needed, headers are complete)
SELECT
    SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber,
    OrderQty, ProductID, SpecialOfferID, UnitPrice,
    UnitPriceDiscount, CAST(LineTotal AS money) AS LineTotal, ModifiedDate
INTO bronze.SalesOrderDetail
FROM AdventureWorks2022.Sales.SalesOrderDetail;
GO

-- bronze.Employee
SELECT
    BusinessEntityID, NationalIDNumber, LoginID, JobTitle, BirthDate,
    MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours,
    SickLeaveHours, CurrentFlag, ModifiedDate
INTO bronze.Employee
FROM AdventureWorks2022.HumanResources.Employee;
GO

-- bronze.CountryRegion
SELECT CountryRegionCode, Name AS CountryRegionName, ModifiedDate
INTO bronze.CountryRegion
FROM AdventureWorks2022.Person.CountryRegion;
GO

-- bronze.StateProvince
SELECT StateProvinceID, StateProvinceCode, CountryRegionCode,
    IsOnlyStateProvinceFlag, Name AS StateProvinceName, TerritoryID, ModifiedDate
INTO bronze.StateProvince
FROM AdventureWorks2022.Person.StateProvince;
GO

-- bronze.Currency
SELECT CurrencyCode, Name AS CurrencyName, ModifiedDate
INTO bronze.Currency
FROM AdventureWorks2022.Sales.Currency;
GO

-- bronze.Promotion (from SpecialOffer)
SELECT SpecialOfferID AS PromotionID, Description, CAST(DiscountPct AS float) AS DiscountPct,
    Type AS PromotionType, Category AS PromotionCategory,
    StartDate, EndDate, MinQty, MaxQty, ModifiedDate
INTO bronze.Promotion
FROM AdventureWorks2022.Sales.SpecialOffer;
GO

-- bronze.SalesTerritory
SELECT TerritoryID, Name AS TerritoryName, CountryRegionCode,
    [Group] AS TerritoryGroup, SalesYTD, SalesLastYear, CostYTD, CostLastYear, ModifiedDate
INTO bronze.SalesTerritory
FROM AdventureWorks2022.Sales.SalesTerritory;
GO
-- ============================================================
-- SILVER TABLES
-- ============================================================

CREATE TABLE silver.DimProduct (
    ProductKey          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductAlternateKey NVARCHAR(25)      NULL,
    EnglishProductName  NVARCHAR(50)      NOT NULL,
    StandardCost        MONEY             NULL,
    ListPrice           MONEY             NULL,
    Color               NVARCHAR(15)      NOT NULL DEFAULT '',
    Size                NVARCHAR(50)      NULL,
    ProductLine         NCHAR(2)          NULL,
    Class               NCHAR(2)          NULL,
    Style               NCHAR(2)          NULL,
    StartDate           DATETIME          NULL,
    EndDate             DATETIME          NULL,
    Status              NVARCHAR(10)      NULL
);
GO

CREATE TABLE silver.DimCustomer (
    CustomerKey         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerAlternateKey NVARCHAR(15)     NOT NULL,
    FirstName           NVARCHAR(50)      NULL,
    MiddleName          NVARCHAR(50)      NULL,
    LastName            NVARCHAR(50)      NULL,
    Title               NVARCHAR(8)       NULL,
    Gender              NVARCHAR(1)       NULL,
    MaritalStatus       NCHAR(1)          NULL,
    EmailPromotion      INT               NULL,
    DateFirstPurchase   DATE              NULL
);
GO

CREATE TABLE silver.DimGeography (
    GeographyKey         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    StateProvinceCode    NVARCHAR(3)       NULL,
    StateProvinceName    NVARCHAR(50)      NULL,
    CountryRegionCode    NVARCHAR(3)       NULL,
    CountryRegionName    NVARCHAR(50)      NULL
);
GO

CREATE TABLE silver.DimCurrency (
    CurrencyKey         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CurrencyAlternateKey NCHAR(3)         NOT NULL,
    CurrencyName        NVARCHAR(50)      NOT NULL
);
GO

CREATE TABLE silver.DimEmployee (
    EmployeeKey              INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    EmployeeAlternateKey     NVARCHAR(15)      NULL,
    FirstName                NVARCHAR(50)      NOT NULL,
    LastName                 NVARCHAR(50)      NOT NULL,
    Title                    NVARCHAR(50)      NULL,
    HireDate                 DATE              NULL,
    Gender                   NCHAR(1)          NULL,
    MaritalStatus            NCHAR(1)          NULL,
    SalariedFlag             BIT               NULL,
    CurrentFlag              BIT               NOT NULL DEFAULT 1
);
GO

CREATE TABLE silver.DimPromotion (
    PromotionKey            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PromotionAlternateKey   INT               NULL,
    EnglishPromotionName    NVARCHAR(255)     NULL,
    DiscountPct             FLOAT             NULL,
    EnglishPromotionType    NVARCHAR(50)      NULL,
    EnglishPromotionCategory NVARCHAR(50)     NULL,
    StartDate               DATETIME          NOT NULL,
    EndDate                 DATETIME          NULL,
    MinQty                  INT               NULL,
    MaxQty                  INT               NULL
);
GO

CREATE TABLE silver.DimSalesTerritory (
    SalesTerritoryKey          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SalesTerritoryAlternateKey INT               NULL,
    SalesTerritoryRegion       NVARCHAR(50)      NOT NULL,
    SalesTerritoryCountry      NVARCHAR(50)      NOT NULL,
    SalesTerritoryGroup        NVARCHAR(50)      NULL
);
GO

CREATE TABLE silver.FactInternetSales (
    SalesOrderNumber    NVARCHAR(20)  NOT NULL,
    SalesOrderLineNumber TINYINT      NOT NULL,
    ProductKey          INT           NOT NULL,
    CustomerKey         INT           NOT NULL,
    SalesTerritoryKey   INT           NULL,
    OrderQuantity       SMALLINT      NOT NULL,
    UnitPrice           MONEY         NOT NULL,
    ExtendedAmount      MONEY         NULL,
    SalesAmount         MONEY         NOT NULL,
    TaxAmt              MONEY         NULL,
    Freight             MONEY         NULL,
    OrderDate           DATETIME      NULL,
    DueDate             DATETIME      NULL,
    ShipDate            DATETIME      NULL,
    CONSTRAINT PK_FactInternetSales PRIMARY KEY (SalesOrderNumber, SalesOrderLineNumber)
);
GO

-- ============================================================
-- VIEWS
-- ============================================================

-- Updateable view over silver.DimPromotion (writer-through-view scenario)
CREATE VIEW silver.vw_DimPromotion
WITH SCHEMABINDING
AS
SELECT
    PromotionKey,
    PromotionAlternateKey,
    EnglishPromotionName,
    DiscountPct,
    EnglishPromotionType,
    EnglishPromotionCategory,
    StartDate,
    EndDate,
    MinQty,
    MaxQty
FROM silver.DimPromotion;
GO

-- Indexed view over silver.DimSalesTerritory (MV-as-target scenario)
SET QUOTED_IDENTIFIER ON;
GO
CREATE VIEW silver.vDimSalesTerritory
WITH SCHEMABINDING
AS
SELECT
    SalesTerritoryKey,
    SalesTerritoryAlternateKey,
    SalesTerritoryRegion,
    SalesTerritoryCountry,
    SalesTerritoryGroup
FROM silver.DimSalesTerritory;
GO

CREATE UNIQUE CLUSTERED INDEX IX_vDimSalesTerritory
ON silver.vDimSalesTerritory (SalesTerritoryKey);
GO
USE MigrationTest;
GO

-- ============================================================
-- SCENARIO: resolved (direct writer via MERGE)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimProduct AS tgt
    USING (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName                     AS EnglishProductName,
            StandardCost,
            ListPrice,
            ISNULL(Color, '')               AS Color,
            Size,
            ProductLine,
            Class,
            Style,
            SellStartDate                   AS StartDate,
            SellEndDate                     AS EndDate,
            CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
                 WHEN SellEndDate IS NOT NULL      THEN 'Outdated'
                 ELSE 'Current' END          AS Status
        FROM bronze.Product
    ) AS src ON tgt.ProductAlternateKey = src.ProductAlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.EnglishProductName = src.EnglishProductName,
        tgt.StandardCost       = src.StandardCost,
        tgt.ListPrice          = src.ListPrice,
        tgt.Color              = src.Color,
        tgt.Size               = src.Size,
        tgt.ProductLine        = src.ProductLine,
        tgt.Class              = src.Class,
        tgt.Style              = src.Style,
        tgt.Status             = src.Status
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        ProductAlternateKey, EnglishProductName, StandardCost, ListPrice,
        Color, Size, ProductLine, Class, Style, StartDate, EndDate, Status)
    VALUES (
        src.ProductAlternateKey, src.EnglishProductName, src.StandardCost, src.ListPrice,
        src.Color, src.Size, src.ProductLine, src.Class, src.Style, src.StartDate, src.EndDate, src.Status);
END;
GO

-- ============================================================
-- SCENARIO: ambiguous_multi_writer — writer A (full reload)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_DimCustomer_Full
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimCustomer;
    INSERT INTO silver.DimCustomer (
        CustomerAlternateKey, FirstName, MiddleName, LastName, Title,
        Gender, MaritalStatus, EmailPromotion, DateFirstPurchase)
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
        p.FirstName,
        p.MiddleName,
        p.LastName,
        p.Title,
        NULL        AS Gender,
        NULL        AS MaritalStatus,
        p.EmailPromotion,
        CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinOrderDate
        FROM bronze.SalesOrderHeader sh
        WHERE sh.CustomerID = c.CustomerID
    ) h;
END;
GO

-- ============================================================
-- SCENARIO: ambiguous_multi_writer — writer B (delta merge)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_DimCustomer_Delta
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimCustomer AS tgt
    USING (
        SELECT
            CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
            p.FirstName, p.MiddleName, p.LastName, p.Title,
            NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion
        FROM bronze.Customer c
        JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    ) AS src ON tgt.CustomerAlternateKey = src.CustomerAlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.FirstName      = src.FirstName,
        tgt.MiddleName     = src.MiddleName,
        tgt.LastName       = src.LastName,
        tgt.Title          = src.Title,
        tgt.EmailPromotion = src.EmailPromotion
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        CustomerAlternateKey, FirstName, MiddleName, LastName,
        Title, Gender, MaritalStatus, EmailPromotion)
    VALUES (
        src.CustomerAlternateKey, src.FirstName, src.MiddleName, src.LastName,
        src.Title, src.Gender, src.MaritalStatus, src.EmailPromotion);
END;
GO

-- ============================================================
-- SCENARIO: resolved via call graph — LEAF (direct INSERT)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_stage_FactInternetSales
AS
BEGIN
    SET NOCOUNT ON;
    -- Truncate first to allow idempotent re-runs
    TRUNCATE TABLE silver.FactInternetSales;
    INSERT INTO silver.FactInternetSales (
        SalesOrderNumber, SalesOrderLineNumber, ProductKey, CustomerKey,
        SalesTerritoryKey, OrderQuantity, UnitPrice, ExtendedAmount,
        SalesAmount, TaxAmt, Freight, OrderDate, DueDate, ShipDate)
    SELECT
        h.SalesOrderNumber,
        CAST(d.SalesOrderDetailID % 127 AS TINYINT)  AS SalesOrderLineNumber,
        d.ProductID                                   AS ProductKey,
        h.CustomerID                                  AS CustomerKey,
        h.TerritoryID                                 AS SalesTerritoryKey,
        d.OrderQty                                    AS OrderQuantity,
        d.UnitPrice,
        CAST(d.UnitPrice * d.OrderQty AS MONEY)       AS ExtendedAmount,
        d.LineTotal                                   AS SalesAmount,
        CAST(h.TaxAmt / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS TaxAmt,
        CAST(h.Freight / COUNT(*) OVER (PARTITION BY h.SalesOrderID) AS MONEY) AS Freight,
        h.OrderDate,
        h.DueDate,
        h.ShipDate
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID;
END;
GO

-- ============================================================
-- SCENARIO: resolved via call graph — ORCHESTRATOR (no direct write)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_FactInternetSales
AS
BEGIN
    SET NOCOUNT ON;
    EXEC silver.usp_stage_FactInternetSales;
END;
GO
USE MigrationTest;
GO

-- ============================================================
-- SCENARIO: partial (dynamic SQL only — no static write visible)
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_DimCurrency
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql NVARCHAR(MAX);
    -- Truncate first
    TRUNCATE TABLE silver.DimCurrency;
    -- All writes via dynamic SQL so scoping agent sees no static INSERT/MERGE
    SET @sql = N'
        INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
        SELECT CurrencyCode, CurrencyName
        FROM MigrationTest.bronze.Currency';
    EXEC sp_executesql @sql;
END;
GO

-- ============================================================
-- SCENARIO: error — cross-database reference
-- usp_load_DimEmployee loads from bronze.Employee, then calls
-- usp_sync_DimEmployee_External which contains a cross-database
-- reference to [OtherDB].[dbo].[EmployeeExtended].
-- The scoping agent detects the cross-db ref by reading the
-- proc body from sys.sql_modules during DetectWriteOperations.
-- ============================================================

-- Helper proc with cross-database reference visible in sys.sql_modules
CREATE OR ALTER PROCEDURE silver.usp_sync_DimEmployee_External
AS
BEGIN
    SET NOCOUNT ON;
    -- References cross-database object: registered in sys.sql_modules body
    -- The scoping agent reads this body and detects OtherDB cross-db reference
    DECLARE @ExtCount INT;
    BEGIN TRY
        SELECT @ExtCount = COUNT(*) FROM [OtherDB].[dbo].[EmployeeExtended];
    END TRY
    BEGIN CATCH
        SET @ExtCount = 0;
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE silver.usp_load_DimEmployee
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimEmployee;
    INSERT INTO silver.DimEmployee (
        EmployeeAlternateKey, FirstName, LastName, Title,
        HireDate, Gender, MaritalStatus, SalariedFlag, CurrentFlag)
    SELECT
        e.NationalIDNumber,
        SUBSTRING(e.LoginID, CHARINDEX(N'\', e.LoginID) + 1, 50),
        e.JobTitle,
        e.JobTitle,
        e.HireDate,
        e.Gender,
        e.MaritalStatus,
        e.SalariedFlag,
        e.CurrentFlag
    FROM bronze.Employee e;

    -- Call helper that has cross-database reference
    EXEC silver.usp_sync_DimEmployee_External;
END;
GO

-- ============================================================
-- SCENARIO: writer through updateable view (Could Have)
-- usp_load_DimPromotion writes to silver.vw_DimPromotion (a view
-- over silver.DimPromotion). The proc is the writer; decomposer
-- resolves the view-to-base-table mapping later.
-- ============================================================
CREATE OR ALTER PROCEDURE silver.usp_load_DimPromotion
AS
BEGIN
    SET NOCOUNT ON;
    -- Write via the updateable view (not directly to the base table)
    DELETE FROM silver.vw_DimPromotion;
    INSERT INTO silver.vw_DimPromotion (
        PromotionAlternateKey, EnglishPromotionName, DiscountPct,
        EnglishPromotionType, EnglishPromotionCategory,
        StartDate, EndDate, MinQty, MaxQty)
    SELECT
        p.PromotionID,
        p.Description,
        p.DiscountPct,
        p.PromotionType,
        p.PromotionCategory,
        p.StartDate,
        p.EndDate,
        p.MinQty,
        p.MaxQty
    FROM bronze.Promotion p;
END;
GO
USE MigrationTest;
GO
PRINT '=== Running smoke tests ===';

-- Load all silver tables
EXEC silver.usp_load_DimProduct;
IF (SELECT COUNT(*) FROM silver.DimProduct) = 0
    THROW 50001, 'FAIL: usp_load_DimProduct produced no rows', 1;
PRINT 'PASS: silver.DimProduct';

EXEC silver.usp_load_DimCustomer_Full;
IF (SELECT COUNT(*) FROM silver.DimCustomer) = 0
    THROW 50002, 'FAIL: usp_load_DimCustomer_Full produced no rows', 1;
PRINT 'PASS: silver.DimCustomer (full load)';

EXEC silver.usp_load_FactInternetSales;
IF (SELECT COUNT(*) FROM silver.FactInternetSales) = 0
    THROW 50003, 'FAIL: usp_load_FactInternetSales produced no rows', 1;
PRINT 'PASS: silver.FactInternetSales';

EXEC silver.usp_load_DimCurrency;
IF (SELECT COUNT(*) FROM silver.DimCurrency) = 0
    THROW 50004, 'FAIL: usp_load_DimCurrency produced no rows', 1;
PRINT 'PASS: silver.DimCurrency';

EXEC silver.usp_load_DimEmployee;
IF (SELECT COUNT(*) FROM silver.DimEmployee) = 0
    THROW 50005, 'FAIL: usp_load_DimEmployee produced no rows', 1;
PRINT 'PASS: silver.DimEmployee';

EXEC silver.usp_load_DimPromotion;
IF (SELECT COUNT(*) FROM silver.DimPromotion) = 0
    THROW 50006, 'FAIL: usp_load_DimPromotion produced no rows', 1;
PRINT 'PASS: silver.DimPromotion';

-- Verify cross-db proc body exists
IF NOT EXISTS (
    SELECT 1 FROM sys.sql_modules sm
    JOIN sys.objects o ON o.object_id = sm.object_id
    WHERE o.name = 'usp_sync_DimEmployee_External'
      AND sm.definition LIKE '%OtherDB%')
    THROW 50007, 'FAIL: cross-db reference not found in usp_sync_DimEmployee_External body', 1;
PRINT 'PASS: cross-db reference body check';

-- Summary row counts
PRINT '';
PRINT '=== Silver table row counts ===';
SELECT 'DimProduct'       AS tbl, COUNT(*) AS rows FROM silver.DimProduct
UNION ALL SELECT 'DimCustomer',     COUNT(*) FROM silver.DimCustomer
UNION ALL SELECT 'FactInternetSales', COUNT(*) FROM silver.FactInternetSales
UNION ALL SELECT 'DimGeography',    COUNT(*) FROM silver.DimGeography
UNION ALL SELECT 'DimCurrency',     COUNT(*) FROM silver.DimCurrency
UNION ALL SELECT 'DimEmployee',     COUNT(*) FROM silver.DimEmployee
UNION ALL SELECT 'DimPromotion',    COUNT(*) FROM silver.DimPromotion
UNION ALL SELECT 'DimSalesTerritory', COUNT(*) FROM silver.DimSalesTerritory;

PRINT '=== Smoke tests passed ===';
GO
