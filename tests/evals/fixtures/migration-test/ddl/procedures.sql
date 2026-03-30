
-- ============================================================
-- SCENARIO: partial (dynamic SQL only — no static write visible)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCurrency
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
-- SCENARIO: ambiguous_multi_writer — writer B (delta merge)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCustomer_Delta
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
-- SCENARIO: ambiguous_multi_writer — writer A (full reload)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCustomer_Full
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


CREATE   PROCEDURE silver.usp_load_DimEmployee
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
-- SCENARIO: resolved (direct writer via MERGE)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimProduct
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
-- SCENARIO: writer through updateable view (Could Have)
-- usp_load_DimPromotion writes to silver.vw_DimPromotion (a view
-- over silver.DimPromotion). The proc is the writer; decomposer
-- resolves the view-to-base-table mapping later.
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimPromotion
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
-- SCENARIO: resolved via call graph — ORCHESTRATOR (no direct write)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_FactInternetSales
AS
BEGIN
    SET NOCOUNT ON;
    EXEC silver.usp_stage_FactInternetSales;
END;

GO


-- ============================================================
-- SCENARIO: resolved via call graph — LEAF (direct INSERT)
-- ============================================================
CREATE   PROCEDURE silver.usp_stage_FactInternetSales
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
-- SCENARIO: error — cross-database reference
-- usp_load_DimEmployee loads from bronze.Employee, then calls
-- usp_sync_DimEmployee_External which contains a cross-database
-- reference to [OtherDB].[dbo].[EmployeeExtended].
-- The scoping agent detects the cross-db ref by reading the
-- proc body from sys.sql_modules during DetectWriteOperations.
-- ============================================================

-- Helper proc with cross-database reference visible in sys.sql_modules
CREATE   PROCEDURE silver.usp_sync_DimEmployee_External
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

