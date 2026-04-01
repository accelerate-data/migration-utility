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
-- Helper table for pattern-coverage procs
-- ============================================================

CREATE TABLE dbo.Config (
    ConfigID    INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ConfigKey   NVARCHAR(100)     NOT NULL,
    ConfigValue NVARCHAR(255)     NULL
);
GO

INSERT INTO dbo.Config (ConfigKey, ConfigValue) VALUES
    ('full_reload', '1'),
    ('default_category', 'General'),
    ('cleanup_threshold', '1000');
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
-- ============================================================
-- Section C: T-SQL pattern-coverage procs (patterns 19-44)
-- These complement the scoping-scenario procs above by
-- exercising every deterministic sqlglot pattern from the
-- T-SQL Parse Classification design doc.
-- ============================================================

USE MigrationTest;
GO

-- Pattern 19: UNION ALL
CREATE OR ALTER PROCEDURE dbo.usp_UnionAll
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID <= 250
    UNION ALL
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID > 250;
END;
GO

-- Pattern 20: UNION (dedup)
CREATE OR ALTER PROCEDURE dbo.usp_Union
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID <= 300
    UNION
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, ISNULL(Color, '')
    FROM bronze.Product WHERE ProductID >= 200;
END;
GO

-- Pattern 21: INTERSECT
CREATE OR ALTER PROCEDURE dbo.usp_Intersect
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode <= 'M'
    INTERSECT
    SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode >= 'E';
END;
GO

-- Pattern 22: EXCEPT
CREATE OR ALTER PROCEDURE dbo.usp_Except
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM bronze.Currency
    EXCEPT
    SELECT CurrencyAlternateKey, CurrencyName FROM silver.DimCurrency;
END;
GO

-- Pattern 23: UNION ALL in CTE branch
CREATE OR ALTER PROCEDURE dbo.usp_UnionAllInCTE
AS
BEGIN
    SET NOCOUNT ON;
    WITH combined AS (
        SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode <= 'M'
        UNION ALL
        SELECT CurrencyCode, CurrencyName FROM bronze.Currency WHERE CurrencyCode > 'M'
    )
    INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM combined;
END;
GO

-- Pattern 24: INNER JOIN (explicit keyword)
CREATE OR ALTER PROCEDURE dbo.usp_InnerJoin
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCustomer (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)), p.FirstName, p.LastName
    FROM bronze.Customer c
    INNER JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID;
END;
GO

-- Pattern 25: FULL OUTER JOIN
CREATE OR ALTER PROCEDURE dbo.usp_FullOuterJoin
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT COALESCE(CAST(c.CustomerID AS NVARCHAR(100)), 'no_customer'),
           COALESCE(p.FirstName, 'no_person')
    FROM bronze.Customer c
    FULL OUTER JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID;
END;
GO

-- Pattern 26: CROSS JOIN
CREATE OR ALTER PROCEDURE dbo.usp_CrossJoin
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT CAST(t.TerritoryID AS NVARCHAR(100)), cur.CurrencyName
    FROM bronze.SalesTerritory t
    CROSS JOIN bronze.Currency cur
    WHERE t.TerritoryID <= 3 AND cur.CurrencyCode IN ('USD', 'EUR', 'GBP');
END;
GO

-- Pattern 27: CROSS APPLY
CREATE OR ALTER PROCEDURE dbo.usp_CrossApply
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.FactInternetSales;
    INSERT INTO silver.FactInternetSales (
        SalesOrderNumber, SalesOrderLineNumber, ProductKey, CustomerKey,
        OrderQuantity, UnitPrice, SalesAmount, OrderDate)
    SELECT h.SalesOrderNumber, CAST(d.SalesOrderDetailID % 127 AS TINYINT),
           d.ProductID, h.CustomerID, d.OrderQty, d.UnitPrice, d.LineTotal, h.OrderDate
    FROM bronze.SalesOrderHeader h
    CROSS APPLY (
        SELECT TOP 5 * FROM bronze.SalesOrderDetail det
        WHERE det.SalesOrderID = h.SalesOrderID
        ORDER BY det.SalesOrderDetailID
    ) d
    WHERE h.SalesOrderID <= 43662;
END;
GO

-- Pattern 28: OUTER APPLY
CREATE OR ALTER PROCEDURE dbo.usp_OuterApply
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCustomer (CustomerAlternateKey, FirstName, LastName, DateFirstPurchase)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)),
           COALESCE(p.FirstName, 'Unknown'),
           COALESCE(p.LastName, 'Unknown'),
           CAST(oh.MinDate AS DATE)
    FROM bronze.Customer c
    LEFT JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinDate
        FROM bronze.SalesOrderHeader sh
        WHERE sh.CustomerID = c.CustomerID
    ) oh;
END;
GO

-- Pattern 29: Self-join
CREATE OR ALTER PROCEDURE dbo.usp_SelfJoin
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT CAST(a.ProductID AS NVARCHAR(100)), b.ProductName
    FROM bronze.Product a
    JOIN bronze.Product b ON a.ProductSubcategoryID = b.ProductSubcategoryID
        AND a.ProductID <> b.ProductID
    WHERE a.ProductSubcategoryID IS NOT NULL AND a.ProductID <= 320;
END;
GO

-- Pattern 30: Derived table in FROM
CREATE OR ALTER PROCEDURE dbo.usp_DerivedTable
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName, StandardCost, ListPrice, Color)
    SELECT sub.AltKey, sub.ProdName, sub.AvgCost, sub.AvgPrice, ''
    FROM (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey,
               ProductName AS ProdName,
               StandardCost AS AvgCost,
               ListPrice AS AvgPrice
        FROM bronze.Product
        WHERE ProductName IS NOT NULL
    ) sub
    JOIN bronze.Product p2 ON sub.AltKey = CAST(p2.ProductID AS NVARCHAR(25));
END;
GO

-- Pattern 31: Scalar subquery in SELECT
CREATE OR ALTER PROCEDURE dbo.usp_ScalarSubquery
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCustomer (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)),
           (SELECT TOP 1 p.FirstName FROM bronze.Person p WHERE p.BusinessEntityID = c.PersonID),
           (SELECT TOP 1 p.LastName FROM bronze.Person p WHERE p.BusinessEntityID = c.PersonID)
    FROM bronze.Customer c
    WHERE c.PersonID IS NOT NULL;
END;
GO

-- Pattern 32: EXISTS subquery
CREATE OR ALTER PROCEDURE dbo.usp_ExistsSubquery
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCustomer (CustomerAlternateKey, FirstName, LastName)
    SELECT CAST(c.CustomerID AS NVARCHAR(15)), p.FirstName, p.LastName
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    WHERE EXISTS (
        SELECT 1 FROM bronze.SalesOrderHeader h WHERE h.CustomerID = c.CustomerID
    );
END;
GO

-- Pattern 33: NOT EXISTS subquery
CREATE OR ALTER PROCEDURE dbo.usp_NotExistsSubquery
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
    SELECT cur.CurrencyCode, cur.CurrencyName
    FROM bronze.Currency cur
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.DimCurrency d WHERE d.CurrencyAlternateKey = cur.CurrencyCode
    );
END;
GO

-- Pattern 34: IN subquery
CREATE OR ALTER PROCEDURE dbo.usp_InSubquery
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimEmployee (EmployeeAlternateKey, FirstName, LastName, Title, HireDate)
    SELECT e.NationalIDNumber,
           SUBSTRING(e.LoginID, CHARINDEX(N'\', e.LoginID) + 1, 50),
           e.JobTitle, e.JobTitle, e.HireDate
    FROM bronze.Employee e
    WHERE e.BusinessEntityID IN (
        SELECT PersonID FROM bronze.Customer WHERE PersonID IS NOT NULL
    );
END;
GO

-- Pattern 35: NOT IN subquery
CREATE OR ALTER PROCEDURE dbo.usp_NotInSubquery
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName, Color)
    SELECT CAST(p.ProductID AS NVARCHAR(25)), p.ProductName, ISNULL(p.Color, '')
    FROM bronze.Product p
    WHERE p.ProductID NOT IN (
        SELECT CAST(d.ProductAlternateKey AS INT)
        FROM silver.DimProduct d
        WHERE d.ProductAlternateKey IS NOT NULL
    );
END;
GO

-- Pattern 36: Recursive CTE
CREATE OR ALTER PROCEDURE dbo.usp_RecursiveCTE
AS
BEGIN
    SET NOCOUNT ON;
    WITH hierarchy AS (
        SELECT ProductID, ProductName, 1 AS lvl
        FROM bronze.Product
        WHERE ProductSubcategoryID IS NULL
        UNION ALL
        SELECT p.ProductID, p.ProductName, h.lvl + 1
        FROM bronze.Product p
        JOIN hierarchy h ON p.ProductSubcategoryID = h.ProductID
        WHERE h.lvl < 3
    )
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT CAST(ProductID AS NVARCHAR(100)), ProductName FROM hierarchy;
END;
GO

-- Pattern 37: UPDATE with CTE prefix
CREATE OR ALTER PROCEDURE dbo.usp_UpdateWithCTE
AS
BEGIN
    SET NOCOUNT ON;
    WITH latest AS (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey, ProductName
        FROM bronze.Product
    )
    UPDATE d
    SET EnglishProductName = l.ProductName
    FROM silver.DimProduct d
    JOIN latest l ON d.ProductAlternateKey = l.AltKey;
END;
GO

-- Pattern 38: DELETE with CTE prefix
CREATE OR ALTER PROCEDURE dbo.usp_DeleteWithCTE
AS
BEGIN
    SET NOCOUNT ON;
    WITH stale AS (
        SELECT ProductAlternateKey
        FROM silver.DimProduct
        WHERE EnglishProductName IS NULL
    )
    DELETE FROM silver.DimProduct
    WHERE ProductAlternateKey IN (SELECT ProductAlternateKey FROM stale);
END;
GO

-- Pattern 39: MERGE with CTE source
CREATE OR ALTER PROCEDURE dbo.usp_MergeWithCTE
AS
BEGIN
    SET NOCOUNT ON;
    WITH src AS (
        SELECT CAST(ProductID AS NVARCHAR(25)) AS AltKey,
               ProductName,
               StandardCost,
               ListPrice,
               ISNULL(Color, '') AS Color
        FROM bronze.Product
    )
    MERGE INTO silver.DimProduct AS tgt
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
CREATE OR ALTER PROCEDURE dbo.usp_GroupingSets
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL_COLORS'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM bronze.Product
    GROUP BY GROUPING SETS ((Color), ());
END;
GO

-- Pattern 41: CUBE
CREATE OR ALTER PROCEDURE dbo.usp_Cube
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL') + '|' + COALESCE(ProductLine, 'ALL'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM bronze.Product
    GROUP BY CUBE (Color, ProductLine);
END;
GO

-- Pattern 42: ROLLUP
CREATE OR ALTER PROCEDURE dbo.usp_Rollup
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT COALESCE(Color, 'ALL') + '|' + COALESCE(Class, 'ALL'),
           CAST(COUNT(*) AS NVARCHAR(50))
    FROM bronze.Product
    GROUP BY ROLLUP (Color, Class);
END;
GO

-- Pattern 43: PIVOT
CREATE OR ALTER PROCEDURE dbo.usp_Pivot
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT pvt.TerritoryGroup, CAST(pvt.[1] + pvt.[2] + pvt.[3] AS NVARCHAR(50))
    FROM (
        SELECT st.TerritoryGroup, st.TerritoryID,
               CAST(st.SalesYTD AS MONEY) AS SalesAmt
        FROM bronze.SalesTerritory st
    ) src
    PIVOT (SUM(SalesAmt) FOR TerritoryID IN ([1], [2], [3])) pvt;
END;
GO

-- Pattern 44: UNPIVOT
CREATE OR ALTER PROCEDURE dbo.usp_Unpivot
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.Config (ConfigKey, ConfigValue)
    SELECT CAST(unpvt.ProductID AS NVARCHAR(100)), unpvt.AttrValue
    FROM (
        SELECT ProductID,
               CAST(ISNULL(ProductName, '') AS NVARCHAR(50)) AS ProductName,
               CAST(ISNULL(ProductNumber, '') AS NVARCHAR(50)) AS ProductNumber
        FROM bronze.Product
        WHERE ProductID <= 10
    ) src
    UNPIVOT (AttrValue FOR AttrName IN (ProductName, ProductNumber)) unpvt;
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
