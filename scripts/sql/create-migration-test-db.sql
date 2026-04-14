-- ============================================================
-- Section A: single-schema fixture bootstrap
-- Idempotent: recreates canonical objects inside [__MSSQL_SCHEMA__]
-- ============================================================

USE [__MSSQL_DB__];
GO

IF SCHEMA_ID(N'__MSSQL_SCHEMA__') IS NULL
    EXEC(N'CREATE SCHEMA [__MSSQL_SCHEMA__]');
GO

DECLARE @schema_name sysname = N'__MSSQL_SCHEMA__';
DECLARE @drop_sql nvarchar(max) = N'';

SELECT @drop_sql = STRING_AGG(
    N'DROP PROCEDURE IF EXISTS [__MSSQL_SCHEMA__].[' + p.name + N'];',
    CHAR(10)
)
FROM sys.procedures AS p
WHERE p.schema_id = SCHEMA_ID(@schema_name);

IF @drop_sql IS NOT NULL AND LEN(@drop_sql) > 0
    EXEC sp_executesql @drop_sql;
GO

DECLARE @schema_name sysname = N'__MSSQL_SCHEMA__';
DECLARE @drop_sql nvarchar(max) = N'';

SELECT @drop_sql = STRING_AGG(
    N'DROP VIEW IF EXISTS [__MSSQL_SCHEMA__].[' + v.name + N'];',
    CHAR(10)
)
FROM sys.views AS v
WHERE v.schema_id = SCHEMA_ID(@schema_name);

IF @drop_sql IS NOT NULL AND LEN(@drop_sql) > 0
    EXEC sp_executesql @drop_sql;
GO

DECLARE @schema_name sysname = N'__MSSQL_SCHEMA__';
DECLARE @drop_sql nvarchar(max) = N'';

SELECT @drop_sql = STRING_AGG(
    N'DROP TABLE IF EXISTS [__MSSQL_SCHEMA__].[' + t.name + N'];',
    CHAR(10)
)
FROM sys.tables AS t
WHERE t.schema_id = SCHEMA_ID(@schema_name);

IF @drop_sql IS NOT NULL AND LEN(@drop_sql) > 0
    EXEC sp_executesql @drop_sql;
GO

-- ============================================================
-- Bronze tables — SELECT INTO from [__MSSQL_DB__]
-- (TOP 5000 on large tables to keep test DB small)
-- ============================================================

-- [__MSSQL_SCHEMA__].[bronze_product]
SELECT
    ProductID, Name AS ProductName, ProductNumber,
    MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint,
    StandardCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode,
    CAST(Weight AS float) AS Weight, DaysToManufacture,
    ProductLine, Class, Style, ProductSubcategoryID, ProductModelID,
    SellStartDate, SellEndDate, DiscontinuedDate, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_product]
FROM [__MSSQL_DB__].Production.Product;
GO

-- [__MSSQL_SCHEMA__].[bronze_customer] (all rows — anchor table for Person and Orders)
SELECT
    CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_customer]
FROM [__MSSQL_DB__].Sales.Customer;
GO

-- [__MSSQL_SCHEMA__].[bronze_person] — only persons referenced by [__MSSQL_SCHEMA__].[bronze_customer] (join-complete)
SELECT
    p.BusinessEntityID, p.PersonType, p.Title, p.FirstName, p.MiddleName, p.LastName,
    p.Suffix, p.EmailPromotion, p.ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_person]
FROM [__MSSQL_DB__].Person.Person p
WHERE p.BusinessEntityID IN (
    SELECT PersonID FROM [__MSSQL_SCHEMA__].[bronze_customer] WHERE PersonID IS NOT NULL
);
GO

-- [__MSSQL_SCHEMA__].[bronze_salesorderheader] (all rows)
SELECT
    SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status,
    OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
    CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID,
    ShipMethodID, CreditCardID, SubTotal, TaxAmt, Freight, TotalDue,
    Comment, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_salesorderheader]
FROM [__MSSQL_DB__].Sales.SalesOrderHeader;
GO

-- [__MSSQL_SCHEMA__].[bronze_salesorderdetail] (all rows — no filter needed, headers are complete)
SELECT
    SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber,
    OrderQty, ProductID, SpecialOfferID, UnitPrice,
    UnitPriceDiscount, CAST(LineTotal AS money) AS LineTotal, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_salesorderdetail]
FROM [__MSSQL_DB__].Sales.SalesOrderDetail;
GO

-- [__MSSQL_SCHEMA__].[bronze_employee]
SELECT
    BusinessEntityID, NationalIDNumber, LoginID, JobTitle, BirthDate,
    MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours,
    SickLeaveHours, CurrentFlag, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_employee]
FROM [__MSSQL_DB__].HumanResources.Employee;
GO

-- [__MSSQL_SCHEMA__].[bronze_countryregion]
SELECT CountryRegionCode, Name AS CountryRegionName, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_countryregion]
FROM [__MSSQL_DB__].Person.CountryRegion;
GO

-- [__MSSQL_SCHEMA__].[bronze_stateprovince]
SELECT StateProvinceID, StateProvinceCode, CountryRegionCode,
    IsOnlyStateProvinceFlag, Name AS StateProvinceName, TerritoryID, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_stateprovince]
FROM [__MSSQL_DB__].Person.StateProvince;
GO

-- [__MSSQL_SCHEMA__].[bronze_currency]
SELECT CurrencyCode, Name AS CurrencyName, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_currency]
FROM [__MSSQL_DB__].Sales.Currency;
GO

-- [__MSSQL_SCHEMA__].[bronze_promotion] (from SpecialOffer)
SELECT SpecialOfferID AS PromotionID, Description, CAST(DiscountPct AS float) AS DiscountPct,
    Type AS PromotionType, Category AS PromotionCategory,
    StartDate, EndDate, MinQty, MaxQty, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_promotion]
FROM [__MSSQL_DB__].Sales.SpecialOffer;
GO

-- [__MSSQL_SCHEMA__].[bronze_salesterritory]
SELECT TerritoryID, Name AS TerritoryName, CountryRegionCode,
    [Group] AS TerritoryGroup, SalesYTD, SalesLastYear, CostYTD, CostLastYear, ModifiedDate
INTO [__MSSQL_SCHEMA__].[bronze_salesterritory]
FROM [__MSSQL_DB__].Sales.SalesTerritory;
GO
-- ============================================================
-- SILVER TABLES
-- ============================================================

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimproduct] (
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

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimcustomer] (
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

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimgeography] (
    GeographyKey         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    StateProvinceCode    NVARCHAR(3)       NULL,
    StateProvinceName    NVARCHAR(50)      NULL,
    CountryRegionCode    NVARCHAR(3)       NULL,
    CountryRegionName    NVARCHAR(50)      NULL
);
GO

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimcurrency] (
    CurrencyKey         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CurrencyAlternateKey NCHAR(3)         NOT NULL,
    CurrencyName        NVARCHAR(50)      NOT NULL
);
GO

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimemployee] (
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

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimpromotion] (
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

CREATE TABLE [__MSSQL_SCHEMA__].[silver_dimsalesterritory] (
    SalesTerritoryKey          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SalesTerritoryAlternateKey INT               NULL,
    SalesTerritoryRegion       NVARCHAR(50)      NOT NULL,
    SalesTerritoryCountry      NVARCHAR(50)      NOT NULL,
    SalesTerritoryGroup        NVARCHAR(50)      NULL
);
GO

CREATE TABLE [__MSSQL_SCHEMA__].[silver_factinternetsales] (
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
-- Helper table for pattern-coverage procs
-- ============================================================

CREATE TABLE [__MSSQL_SCHEMA__].[silver_config] (
    ConfigID    INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ConfigKey   NVARCHAR(100)     NOT NULL,
    ConfigValue NVARCHAR(255)     NULL
);
GO

INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue) VALUES
    ('full_reload', '1'),
    ('default_category', 'General'),
    ('cleanup_threshold', '1000');
GO

-- ============================================================
-- VIEWS
-- ============================================================

-- Updateable view over [__MSSQL_SCHEMA__].[silver_dimpromotion] (writer-through-view scenario)
CREATE VIEW [__MSSQL_SCHEMA__].[silver_vw_dimpromotion]
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
FROM [__MSSQL_SCHEMA__].[silver_dimpromotion];
GO

-- Indexed view over [__MSSQL_SCHEMA__].[silver_dimsalesterritory] (MV-as-target scenario)
SET QUOTED_IDENTIFIER ON;
GO
CREATE VIEW [__MSSQL_SCHEMA__].[silver_vdimsalesterritory]
WITH SCHEMABINDING
AS
SELECT
    SalesTerritoryKey,
    SalesTerritoryAlternateKey,
    SalesTerritoryRegion,
    SalesTerritoryCountry,
    SalesTerritoryGroup
FROM [__MSSQL_SCHEMA__].[silver_dimsalesterritory];
GO

CREATE UNIQUE CLUSTERED INDEX IX_vDimSalesTerritory
ON [__MSSQL_SCHEMA__].[silver_vdimsalesterritory] (SalesTerritoryKey);
GO
USE [__MSSQL_DB__];
GO

-- ============================================================
-- SCENARIO: resolved (direct writer via MERGE)
-- ============================================================
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimproduct]
AS
BEGIN
    SET NOCOUNT ON;
    MERGE [__MSSQL_SCHEMA__].[silver_dimproduct] AS tgt
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
        FROM [__MSSQL_SCHEMA__].[bronze_product]
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
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimcustomer_full]
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [__MSSQL_SCHEMA__].[silver_dimcustomer];
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcustomer] (
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
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinOrderDate
        FROM [__MSSQL_SCHEMA__].[bronze_salesorderheader] sh
        WHERE sh.CustomerID = c.CustomerID
    ) h;
END;
GO

-- ============================================================
-- SCENARIO: ambiguous_multi_writer — writer B (delta merge)
-- ============================================================
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimcustomer_delta]
AS
BEGIN
    SET NOCOUNT ON;
    MERGE [__MSSQL_SCHEMA__].[silver_dimcustomer] AS tgt
    USING (
        SELECT
            CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
            p.FirstName, p.MiddleName, p.LastName, p.Title,
            NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion
        FROM [__MSSQL_SCHEMA__].[bronze_customer] c
        JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID
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
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_stage_factinternetsales]
AS
BEGIN
    SET NOCOUNT ON;
    -- Truncate first to allow idempotent re-runs
    TRUNCATE TABLE [__MSSQL_SCHEMA__].[silver_factinternetsales];
    INSERT INTO [__MSSQL_SCHEMA__].[silver_factinternetsales] (
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
    FROM [__MSSQL_SCHEMA__].[bronze_salesorderheader] h
    JOIN [__MSSQL_SCHEMA__].[bronze_salesorderdetail] d ON h.SalesOrderID = d.SalesOrderID;
END;
GO

-- ============================================================
-- SCENARIO: resolved via call graph — ORCHESTRATOR (no direct write)
-- ============================================================
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_factinternetsales]
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [__MSSQL_SCHEMA__].[silver_usp_stage_factinternetsales];
END;
GO
USE [__MSSQL_DB__];
GO

-- ============================================================
-- SCENARIO: partial (dynamic SQL only — no static write visible)
-- ============================================================
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimcurrency]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql NVARCHAR(MAX);
    -- Truncate first
    TRUNCATE TABLE [__MSSQL_SCHEMA__].[silver_dimcurrency];
    -- All writes via dynamic SQL so scoping agent sees no static INSERT/MERGE
    SET @sql = N'
        INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcurrency] (CurrencyAlternateKey, CurrencyName)
        SELECT CurrencyCode, CurrencyName
        FROM [__MSSQL_SCHEMA__].[bronze_currency]';
    EXEC sp_executesql @sql;
END;
GO

-- ============================================================
-- SCENARIO: error — cross-database reference
-- usp_load_DimEmployee loads from [__MSSQL_SCHEMA__].[bronze_employee], then calls
-- usp_sync_DimEmployee_External which contains a cross-database
-- reference to [OtherDB].[dbo].[EmployeeExtended].
-- The scoping agent detects the cross-db ref by reading the
-- proc body from sys.sql_modules during DetectWriteOperations.
-- ============================================================

-- Helper proc with cross-database reference visible in sys.sql_modules
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_sync_dimemployee_external]
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

CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimemployee]
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [__MSSQL_SCHEMA__].[silver_dimemployee];
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimemployee] (
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
    FROM [__MSSQL_SCHEMA__].[bronze_employee] e;

    -- Call helper that has cross-database reference
    EXEC [__MSSQL_SCHEMA__].[silver_usp_sync_dimemployee_external];
END;
GO

-- ============================================================
-- SCENARIO: writer through updateable view (Could Have)
-- usp_load_DimPromotion writes to [__MSSQL_SCHEMA__].[silver_vw_dimpromotion] (a view
-- over [__MSSQL_SCHEMA__].[silver_dimpromotion]). The proc is the writer; decomposer
-- resolves the view-to-base-table mapping later.
-- ============================================================
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_load_dimpromotion]
AS
BEGIN
    SET NOCOUNT ON;
    -- Write via the updateable view (not directly to the base table)
    DELETE FROM [__MSSQL_SCHEMA__].[silver_vw_dimpromotion];
    INSERT INTO [__MSSQL_SCHEMA__].[silver_vw_dimpromotion] (
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
    FROM [__MSSQL_SCHEMA__].[bronze_promotion] p;
END;
GO
-- ============================================================
-- Section C: T-SQL pattern-coverage procs (patterns 19-44)
-- These complement the scoping-scenario procs above by
-- exercising every deterministic sqlglot pattern from the
-- T-SQL Parse Classification design doc.
-- ============================================================

USE [__MSSQL_DB__];
GO

-- Pattern 19: UNION ALL
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_unionall]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimproduct] (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM [__MSSQL_SCHEMA__].[bronze_product] WHERE ProductID <= 250
    UNION ALL
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM [__MSSQL_SCHEMA__].[bronze_product] WHERE ProductID > 250;
END;
GO

-- Pattern 20: UNION (dedup)
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_union]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimproduct] (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM [__MSSQL_SCHEMA__].[bronze_product] WHERE ProductID <= 300
    UNION
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM [__MSSQL_SCHEMA__].[bronze_product] WHERE ProductID >= 200;
END;
GO

-- Pattern 21: INTERSECT
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_intersect]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcurrency] (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM [__MSSQL_SCHEMA__].[bronze_currency] WHERE CurrencyCode <= 'M'
    INTERSECT
    SELECT CurrencyCode, CurrencyName FROM [__MSSQL_SCHEMA__].[bronze_currency] WHERE CurrencyCode >= 'E';
END;
GO

-- Pattern 22: EXCEPT
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_except]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcurrency] (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM [__MSSQL_SCHEMA__].[bronze_currency]
    EXCEPT
    SELECT CurrencyAlternateKey, CurrencyName FROM [__MSSQL_SCHEMA__].[silver_dimcurrency];
END;
GO

-- Pattern 23: UNION ALL in CTE branch
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_unionallincte]
AS
BEGIN
    SET NOCOUNT ON;
    WITH combined AS (
        SELECT CurrencyCode, CurrencyName FROM [__MSSQL_SCHEMA__].[bronze_currency] WHERE CurrencyCode <= 'M'
        UNION ALL
        SELECT CurrencyCode, CurrencyName FROM [__MSSQL_SCHEMA__].[bronze_currency] WHERE CurrencyCode > 'M'
    )
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcurrency] (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM combined;
END;
GO

-- Pattern 24: INNER JOIN (explicit keyword)
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_innerjoin]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcustomer] (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)), p.FirstName, p.LastName
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    INNER JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID;
END;
GO

-- Pattern 25: FULL OUTER JOIN
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_fullouterjoin]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT COALESCE(CAST(c.CustomerID AS NVARCHAR(100)), 'no_customer'),
           COALESCE(p.FirstName, 'no_person')
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    FULL OUTER JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID;
END;
GO

-- Pattern 26: CROSS JOIN
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_crossjoin]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT CAST(t.TerritoryID AS NVARCHAR(100)), cur.CurrencyName
    FROM [__MSSQL_SCHEMA__].[bronze_salesterritory] t
    CROSS JOIN [__MSSQL_SCHEMA__].[bronze_currency] cur
    WHERE t.TerritoryID <= 3 AND cur.CurrencyCode IN ('USD', 'EUR', 'GBP');
END;
GO

-- Pattern 27: CROSS APPLY
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_crossapply]
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [__MSSQL_SCHEMA__].[silver_factinternetsales];
    INSERT INTO [__MSSQL_SCHEMA__].[silver_factinternetsales] (
        SalesOrderNumber, SalesOrderLineNumber, ProductKey, CustomerKey,
        OrderQuantity, UnitPrice, SalesAmount, OrderDate)
    SELECT h.SalesOrderNumber, CAST(d.SalesOrderDetailID % 127 AS TINYINT),
           d.ProductID, h.CustomerID, d.OrderQty, d.UnitPrice, d.LineTotal, h.OrderDate
    FROM [__MSSQL_SCHEMA__].[bronze_salesorderheader] h
    CROSS APPLY (
        SELECT TOP 5 * FROM [__MSSQL_SCHEMA__].[bronze_salesorderdetail] det
        WHERE det.SalesOrderID = h.SalesOrderID
        ORDER BY det.SalesOrderDetailID
    ) d
    WHERE h.SalesOrderID <= 43662;
END;
GO

-- Pattern 28: OUTER APPLY
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_outerapply]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcustomer] (CustomerAlternateKey, FirstName, LastName, DateFirstPurchase)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)),
           COALESCE(p.FirstName, 'Unknown'),
           COALESCE(p.LastName, 'Unknown'),
           CAST(oh.MinDate AS DATE)
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    LEFT JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinDate
        FROM [__MSSQL_SCHEMA__].[bronze_salesorderheader] sh
        WHERE sh.CustomerID = c.CustomerID
    ) oh;
END;
GO

-- Pattern 29: Self-join
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_selfjoin]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT CAST(a.ProductID AS NVARCHAR(100)), b.ProductName
    FROM [__MSSQL_SCHEMA__].[bronze_product] a
    JOIN [__MSSQL_SCHEMA__].[bronze_product] b ON a.ProductSubcategoryID = b.ProductSubcategoryID
        AND a.ProductID <> b.ProductID
    WHERE a.ProductSubcategoryID IS NOT NULL AND a.ProductID <= 320;
END;
GO

-- Pattern 30: Derived table in FROM
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_derivedtable]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimproduct] (ProductAlternateKey, EnglishProductName, StandardCost, ListPrice, Color)
    SELECT sub.AltKey, sub.ProdName, sub.AvgCost, sub.AvgPrice, ''
    FROM (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey,
               ProductName AS ProdName,
               StandardCost AS AvgCost,
               ListPrice AS AvgPrice
        FROM [__MSSQL_SCHEMA__].[bronze_product]
        WHERE ProductName IS NOT NULL
    ) sub
    JOIN [__MSSQL_SCHEMA__].[bronze_product] p2 ON sub.AltKey = CAST(p2.ProductID AS NVARCHAR(25));
END;
GO

-- Pattern 31: Scalar subquery in SELECT
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_scalarsubquery]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcustomer] (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)),
           (SELECT TOP 1 p.FirstName FROM [__MSSQL_SCHEMA__].[bronze_person] p WHERE p.BusinessEntityID = c.PersonID),
           (SELECT TOP 1 p.LastName FROM [__MSSQL_SCHEMA__].[bronze_person] p WHERE p.BusinessEntityID = c.PersonID)
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    WHERE c.PersonID IS NOT NULL;
END;
GO

-- Pattern 32: EXISTS subquery
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_existssubquery]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcustomer] (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)), p.FirstName, p.LastName
    FROM [__MSSQL_SCHEMA__].[bronze_customer] c
    JOIN [__MSSQL_SCHEMA__].[bronze_person] p ON c.PersonID = p.BusinessEntityID
    WHERE EXISTS (
        SELECT 1 FROM [__MSSQL_SCHEMA__].[bronze_salesorderheader] h WHERE h.CustomerID = c.CustomerID
    );
END;
GO

-- Pattern 33: NOT EXISTS subquery
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_notexistssubquery]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimcurrency] (CurrencyAlternateKey, CurrencyName)
    SELECT cur.CurrencyCode, cur.CurrencyName
    FROM [__MSSQL_SCHEMA__].[bronze_currency] cur
    WHERE NOT EXISTS (
        SELECT 1 FROM [__MSSQL_SCHEMA__].[silver_dimcurrency] d WHERE d.CurrencyAlternateKey = cur.CurrencyCode
    );
END;
GO

-- Pattern 34: IN subquery
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_insubquery]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimemployee] (EmployeeAlternateKey, FirstName, LastName, Title, HireDate)
    SELECT e.NationalIDNumber,
           SUBSTRING(e.LoginID, CHARINDEX(N'\', e.LoginID) + 1, 50),
           e.JobTitle, e.JobTitle, e.HireDate
    FROM [__MSSQL_SCHEMA__].[bronze_employee] e
    WHERE e.BusinessEntityID IN (
        SELECT PersonID FROM [__MSSQL_SCHEMA__].[bronze_customer] WHERE PersonID IS NOT NULL
    );
END;
GO

-- Pattern 35: NOT IN subquery
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_notinsubquery]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_dimproduct] (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(p.ProductID AS NVARCHAR(25)), p.ProductName, ISNULL(p.Color, '')
    FROM [__MSSQL_SCHEMA__].[bronze_product] p
    WHERE p.ProductID NOT IN (
        SELECT CAST(d.ProductAlternateKey AS INT)
        FROM [__MSSQL_SCHEMA__].[silver_dimproduct] d
        WHERE d.ProductAlternateKey IS NOT NULL
    );
END;
GO

-- Pattern 36: Recursive CTE
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_recursivecte]
AS
BEGIN
    SET NOCOUNT ON;
    WITH hierarchy AS (
        SELECT ProductID, ProductName, 1 AS lvl
        FROM [__MSSQL_SCHEMA__].[bronze_product]
        WHERE ProductSubcategoryID IS NULL
        UNION ALL
        SELECT p.ProductID, p.ProductName, h.lvl + 1
        FROM [__MSSQL_SCHEMA__].[bronze_product] p
        JOIN hierarchy h ON p.ProductSubcategoryID = h.ProductID
        WHERE h.lvl < 3
    )
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT CAST(ProductID AS NVARCHAR(100)), ProductName FROM hierarchy;
END;
GO

-- Pattern 37: UPDATE with CTE prefix
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_updatewithcte]
AS
BEGIN
    SET NOCOUNT ON;
    WITH latest AS (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName
        FROM [__MSSQL_SCHEMA__].[bronze_product]
    )
    UPDATE d
    SET EnglishProductName = l.ProductName
    FROM [__MSSQL_SCHEMA__].[silver_dimproduct] d
    JOIN latest l ON d.ProductAlternateKey = l.AltKey;
END;
GO

-- Pattern 38: DELETE with CTE prefix
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_deletewithcte]
AS
BEGIN
    SET NOCOUNT ON;
    WITH stale AS (
        SELECT ProductAlternateKey
        FROM [__MSSQL_SCHEMA__].[silver_dimproduct]
        WHERE EnglishProductName IS NULL
    )
    DELETE FROM [__MSSQL_SCHEMA__].[silver_dimproduct]
    WHERE ProductAlternateKey IN (SELECT ProductAlternateKey FROM stale);
END;
GO

-- Pattern 39: MERGE with CTE source
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_mergewithcte]
AS
BEGIN
    SET NOCOUNT ON;
    WITH src AS (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey,
               ProductName,
               StandardCost,
               ListPrice,
               ISNULL(Color, '') AS Color
        FROM [__MSSQL_SCHEMA__].[bronze_product]
    )
    MERGE INTO [__MSSQL_SCHEMA__].[silver_dimproduct] AS tgt
    USING src ON tgt.ProductAlternateKey = src.AltKey
    WHEN MATCHED THEN
        UPDATE SET tgt.EnglishProductName = src.ProductName,
                   tgt.StandardCost = src.StandardCost,
                   tgt.ListPrice = src.ListPrice,
                   tgt.Color = src.Color
    WHEN NOT MATCHED THEN
        INSERT (ProductAlternateKey, EnglishProductName, StandardCost, ListPrice, Color)
        VALUES (src.AltKey, src.ProductName, src.StandardCost, src.ListPrice, src.Color);
END;
GO

-- Pattern 40: GROUPING SETS
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_groupingsets]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL_COLORS'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM [__MSSQL_SCHEMA__].[bronze_product]
    GROUP BY GROUPING SETS ((Color), ());
END;
GO

-- Pattern 41: CUBE
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_cube]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL') + '|' + COALESCE(ProductLine, 'ALL'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM [__MSSQL_SCHEMA__].[bronze_product]
    GROUP BY CUBE (Color, ProductLine);
END;
GO

-- Pattern 42: ROLLUP
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_rollup]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL') + '|' + COALESCE(Class, 'ALL'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM [__MSSQL_SCHEMA__].[bronze_product]
    GROUP BY ROLLUP (Color, Class);
END;
GO

-- Pattern 43: PIVOT
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_pivot]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT pvt.TerritoryGroup, CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50))
    FROM (
        SELECT st.TerritoryGroup, st.TerritoryID,
               CAST(st.SalesYTD AS MONEY) AS SalesAmt
        FROM [__MSSQL_SCHEMA__].[bronze_salesterritory] st
    ) src
    PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt;
END;
GO

-- Pattern 44: UNPIVOT
CREATE OR ALTER PROCEDURE [__MSSQL_SCHEMA__].[silver_usp_unpivot]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [__MSSQL_SCHEMA__].[silver_config] (ConfigKey, ConfigValue)
    SELECT CAST(unpvt.ProductID AS NVARCHAR(100)), unpvt.AttrValue
    FROM (
        SELECT ProductID,
               CAST(ISNULL(ProductName, '') AS NVARCHAR(50)) AS ProductName,
               CAST(ISNULL(ProductNumber, '') AS NVARCHAR(50)) AS ProductNumber
        FROM [__MSSQL_SCHEMA__].[bronze_product]
        WHERE ProductID <= 10
    ) src
    UNPIVOT (AttrValue FOR AttrName IN (ProductName, ProductNumber)) unpvt;
END;
GO

USE [__MSSQL_DB__];
GO
PRINT '=== Running smoke tests ===';

-- Load all silver tables
EXEC [__MSSQL_SCHEMA__].[silver_usp_load_dimproduct];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimproduct]) = 0
    THROW 50001, 'FAIL: usp_load_DimProduct produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_dimproduct]';

EXEC [__MSSQL_SCHEMA__].[silver_usp_load_dimcustomer_full];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimcustomer]) = 0
    THROW 50002, 'FAIL: usp_load_DimCustomer_Full produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_dimcustomer] (full load)';

EXEC [__MSSQL_SCHEMA__].[silver_usp_load_factinternetsales];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_factinternetsales]) = 0
    THROW 50003, 'FAIL: usp_load_FactInternetSales produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_factinternetsales]';

EXEC [__MSSQL_SCHEMA__].[silver_usp_load_dimcurrency];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimcurrency]) = 0
    THROW 50004, 'FAIL: usp_load_DimCurrency produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_dimcurrency]';

EXEC [__MSSQL_SCHEMA__].[silver_usp_load_dimemployee];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimemployee]) = 0
    THROW 50005, 'FAIL: usp_load_DimEmployee produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_dimemployee]';

EXEC [__MSSQL_SCHEMA__].[silver_usp_load_dimpromotion];
IF (SELECT COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimpromotion]) = 0
    THROW 50006, 'FAIL: usp_load_DimPromotion produced no rows', 1;
PRINT 'PASS: [__MSSQL_SCHEMA__].[silver_dimpromotion]';

-- Verify cross-db proc body exists
IF NOT EXISTS (
    SELECT 1 FROM sys.sql_modules sm
    JOIN sys.objects o ON o.object_id = sm.object_id
    WHERE o.name = 'silver_usp_sync_dimemployee_external'
      AND sm.definition LIKE '%OtherDB%')
    THROW 50007, 'FAIL: cross-db reference not found in usp_sync_DimEmployee_External body', 1;
PRINT 'PASS: cross-db reference body check';

-- Summary row counts
PRINT '';
PRINT '=== Silver table row counts ===';
SELECT 'DimProduct'       AS tbl, COUNT(*) AS rows FROM [__MSSQL_SCHEMA__].[silver_dimproduct]
UNION ALL SELECT 'DimCustomer',     COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimcustomer]
UNION ALL SELECT 'FactInternetSales', COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_factinternetsales]
UNION ALL SELECT 'DimGeography',    COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimgeography]
UNION ALL SELECT 'DimCurrency',     COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimcurrency]
UNION ALL SELECT 'DimEmployee',     COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimemployee]
UNION ALL SELECT 'DimPromotion',    COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimpromotion]
UNION ALL SELECT 'DimSalesTerritory', COUNT(*) FROM [__MSSQL_SCHEMA__].[silver_dimsalesterritory];

PRINT '=== Smoke tests passed ===';
GO
