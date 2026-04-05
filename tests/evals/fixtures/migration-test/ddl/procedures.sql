
-- ============================================================
-- SCENARIO: single CTE — INSERT via WITH clause (active filter)
-- ============================================================
CREATE PROCEDURE silver.usp_load_SingleCteTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.SingleCteTarget;
    WITH active_products AS (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName                     AS EnglishProductName
        FROM bronze.Product
        WHERE SellEndDate IS NULL
    )
    INSERT INTO silver.SingleCteTarget (ProductAlternateKey, EnglishProductName)
    SELECT ProductAlternateKey, EnglishProductName
    FROM active_products;
END;

GO


-- ============================================================
-- SCENARIO: INSERT INTO ... SELECT — simple full-refresh insert
-- ============================================================
CREATE PROCEDURE silver.usp_load_InsertSelectTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.InsertSelectTarget;
    INSERT INTO silver.InsertSelectTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName                     AS EnglishProductName
    FROM bronze.Product;
END;

GO


-- ============================================================
-- SCENARIO: SELECT INTO — full-refresh via SELECT INTO pattern
-- ============================================================
CREATE PROCEDURE silver.usp_load_SelectIntoTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.SelectIntoTarget;
    SELECT
        CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
        ProductName AS EnglishProductName
    INTO silver.SelectIntoTarget
    FROM bronze.Product;
END;

GO


-- ============================================================
-- SCENARIO: UPDATE ... FROM JOIN — rewrite as source-driven select
-- ============================================================
CREATE PROCEDURE silver.usp_load_UpdateJoinTarget
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE tgt
    SET
        tgt.EnglishProductName = src.ProductName,
        tgt.LastSeenDate = GETDATE()
    FROM silver.UpdateJoinTarget AS tgt
    INNER JOIN bronze.Product AS src
        ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25));
END;

GO


-- ============================================================
-- SCENARIO: DELETE ... WHERE — keep-rows projection
-- ============================================================
CREATE PROCEDURE silver.usp_load_DeleteWhereTarget
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM silver.DeleteWhereTarget
    WHERE IsRetired = 1;
END;

GO


-- ============================================================
-- SCENARIO: correlated subquery — preserve MAX-per-name filter
-- ============================================================
CREATE PROCEDURE silver.usp_load_CorrelatedSubqueryTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.CorrelatedSubqueryTarget;
    INSERT INTO silver.CorrelatedSubqueryTarget (ProductAlternateKey, EnglishProductName)
    SELECT
        CAST(p.ProductID AS NVARCHAR(25)),
        p.ProductName
    FROM bronze.Product AS p
    WHERE p.ProductID = (
        SELECT MAX(p2.ProductID)
        FROM bronze.Product AS p2
        WHERE p2.ProductName = p.ProductName
    );
END;

GO


-- ============================================================
-- SCENARIO: UNION ALL — preserve segmented branches
-- ============================================================
CREATE PROCEDURE silver.usp_load_UnionAllTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.UnionAllTarget;
    INSERT INTO silver.UnionAllTarget (ProductAlternateKey, EnglishProductName, Segment)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, 'red'
    FROM bronze.Product
    WHERE Color = 'Red'
    UNION ALL
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName, 'other'
    FROM bronze.Product
    WHERE Color <> 'Red' OR Color IS NULL;
END;

GO


-- ============================================================
-- SCENARIO: GROUPING SETS — subtotal plus grand total
-- ============================================================
CREATE PROCEDURE silver.usp_load_GroupingSetsTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.GroupingSetsTarget;
    INSERT INTO silver.GroupingSetsTarget (GroupKey, ProductCount)
    SELECT
        COALESCE(Color, 'all'),
        COUNT(*)
    FROM bronze.Product
    GROUP BY GROUPING SETS ((Color), ());
END;

GO


-- ============================================================
-- SCENARIO: PIVOT — color counts become columns
-- ============================================================
CREATE PROCEDURE silver.usp_load_PivotTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.PivotTarget;
    INSERT INTO silver.PivotTarget (MetricName, RedCount, BlackCount, SilverCount)
    SELECT
        'product_counts',
        ISNULL([Red], 0),
        ISNULL([Black], 0),
        ISNULL([Silver], 0)
    FROM (
        SELECT Color, ProductID
        FROM bronze.Product
    ) AS src
    PIVOT (
        COUNT(ProductID) FOR Color IN ([Red], [Black], [Silver])
    ) AS p;
END;

GO


-- ============================================================
-- SCENARIO: IF/ELSE — conditional insert based on price threshold
-- ============================================================
CREATE PROCEDURE silver.usp_load_IfElseTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.IfElseTarget;
    IF (SELECT AVG(ListPrice) FROM bronze.Product) > 100
    BEGIN
        INSERT INTO silver.IfElseTarget (ProductAlternateKey, EnglishProductName, PriceCategory)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName,
            'Premium'
        FROM bronze.Product
        WHERE ListPrice > 100;
    END
    ELSE
    BEGIN
        INSERT INTO silver.IfElseTarget (ProductAlternateKey, EnglishProductName, PriceCategory)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName,
            'Standard'
        FROM bronze.Product;
    END;
END;

GO


-- ============================================================
-- SCENARIO: WHILE — batched insert with loop counter
-- ============================================================
CREATE PROCEDURE silver.usp_load_WhileTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.WhileTarget;
    DECLARE @BatchId INT = 1;
    DECLARE @MaxBatch INT = (SELECT CEILING(COUNT(*) / 100.0) FROM bronze.Product);
    WHILE @BatchId <= @MaxBatch
    BEGIN
        INSERT INTO silver.WhileTarget (BatchId, ProductAlternateKey, EnglishProductName)
        SELECT
            @BatchId,
            CAST(ProductID AS NVARCHAR(25)),
            ProductName
        FROM bronze.Product
        ORDER BY ProductID
        OFFSET (@BatchId - 1) * 100 ROWS FETCH NEXT 100 ROWS ONLY;
        SET @BatchId = @BatchId + 1;
    END;
END;

GO


-- ============================================================
-- SCENARIO: static sp_executesql — literal SQL string passed directly
-- ============================================================
CREATE PROCEDURE silver.usp_load_StaticSpExecTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.StaticSpExecTarget;
    EXEC sp_executesql N'INSERT INTO silver.StaticSpExecTarget (ProductAlternateKey, EnglishProductName)
        SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product';
END;

GO


-- ============================================================
-- SCENARIO: cross-db exec — writer delegates to another database
-- usp_load_DimCrossDbProfile only EXECs a cross-database
-- procedure, so the profiler cannot inspect the write pattern.
-- Profile status stays partial because the proc body is opaque.
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCrossDbProfile
AS
BEGIN
    SET NOCOUNT ON;
    -- All write logic is in a cross-database procedure; body is opaque
    EXEC [ArchiveDB].[silver].[usp_stage_DimCrossDbProfile];
END;

GO


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



-- ============================================================
-- SCENARIO: exec-call-chain — load proc delegates to stage proc
-- ============================================================
CREATE PROCEDURE [silver].[usp_load_FactExecProfile]
AS
BEGIN
    SET NOCOUNT ON;
    EXEC silver.usp_stage_FactExecProfile;
END;

GO


-- ============================================================
-- SCENARIO: exec-call-chain — stage proc performs the actual INSERT
-- ============================================================
CREATE PROCEDURE [silver].[usp_stage_FactExecProfile]
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO silver.FactExecProfile (
        ProcedureKey,
        ExecutionDate,
        DurationMs,
        RowsAffected,
        StatusCode
    )
    SELECT
        p.ProcedureKey,
        e.ExecutionDate,
        e.DurationMs,
        e.RowsAffected,
        e.StatusCode
    FROM bronze.ExecLog e
    JOIN silver.DimProcedure p ON e.ProcedureName = p.ProcedureName;
END;

GO


-- ============================================================
-- SCENARIO: linked-server EXEC — four-part name is out-of-scope
-- ============================================================
CREATE PROCEDURE silver.usp_scope_LinkedServerExec
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [LinkedServer].[WarehouseDb].[silver].[usp_remote_LoadProduct];
END;

GO


-- ============================================================
-- SCENARIO: IF/ELSE — conditional branches write to same target
-- ============================================================
CREATE PROCEDURE silver.usp_load_IfElseTarget
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @HasRecords BIT;
    SELECT @HasRecords = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
    FROM silver.IfElseTarget;

    IF @HasRecords = 1
    BEGIN
        UPDATE tgt
        SET tgt.EnglishProductName = src.ProductName,
            tgt.ModifiedDate = GETDATE()
        FROM silver.IfElseTarget AS tgt
        INNER JOIN bronze.Product AS src
            ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25));
    END
    ELSE
    BEGIN
        INSERT INTO silver.IfElseTarget (ProductAlternateKey, EnglishProductName, ModifiedDate)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName,
            GETDATE()
        FROM bronze.Product;
    END;
END;

GO


-- ============================================================
-- SCENARIO: WHILE loop — iterative batch insert
-- ============================================================
CREATE PROCEDURE silver.usp_load_WhileLoopTarget
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @BatchSize INT = 1000;
    DECLARE @Offset INT = 0;
    DECLARE @RowCount INT = 1;

    TRUNCATE TABLE silver.WhileLoopTarget;

    WHILE @RowCount > 0
    BEGIN
        INSERT INTO silver.WhileLoopTarget (ProductAlternateKey, EnglishProductName)
        SELECT
            CAST(ProductID AS NVARCHAR(25)),
            ProductName
        FROM bronze.Product
        ORDER BY ProductID
        OFFSET @Offset ROWS FETCH NEXT @BatchSize ROWS ONLY;

        SET @RowCount = @@ROWCOUNT;
        SET @Offset = @Offset + @BatchSize;
    END;
END;

GO


-- ============================================================
-- SCENARIO: static sp_executesql — literal SQL string resolved
-- ============================================================
CREATE PROCEDURE silver.usp_load_StaticSpExecTarget
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.StaticSpExecTarget;
    EXEC sp_executesql N'
        INSERT INTO silver.StaticSpExecTarget (ProductAlternateKey, EnglishProductName)
        SELECT CAST(ProductID AS NVARCHAR(25)), ProductName
        FROM bronze.Product';
END;

GO


-- ============================================================
-- SCENARIO: SCD Type 2 — MERGE with expire + insert pattern
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimEmployeeSCD2
AS
BEGIN
    SET NOCOUNT ON;
    -- Expire changed rows
    UPDATE tgt
    SET tgt.ValidTo = GETDATE(),
        tgt.IsCurrent = 0
    FROM silver.DimEmployeeSCD2 AS tgt
    INNER JOIN bronze.Employee AS src
        ON tgt.EmployeeNaturalKey = src.NationalIDNumber
    WHERE tgt.IsCurrent = 1
      AND (tgt.JobTitle <> src.JobTitle OR tgt.Department <> src.MaritalStatus);

    -- Insert new current rows for changed employees
    INSERT INTO silver.DimEmployeeSCD2 (
        EmployeeNaturalKey, FirstName, LastName, JobTitle, Department,
        ValidFrom, ValidTo, IsCurrent)
    SELECT
        src.NationalIDNumber,
        SUBSTRING(src.LoginID, CHARINDEX(N'\', src.LoginID) + 1, 50),
        src.JobTitle,
        src.JobTitle,
        src.MaritalStatus,
        GETDATE(),
        CAST('9999-12-31' AS DATETIME2),
        1
    FROM bronze.Employee AS src
    LEFT JOIN silver.DimEmployeeSCD2 AS tgt
        ON src.NationalIDNumber = tgt.EmployeeNaturalKey
       AND tgt.IsCurrent = 1
    WHERE tgt.EmployeeSCD2Key IS NULL;
END;

GO


-- ============================================================
-- SCENARIO: periodic snapshot — daily inventory snapshot
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactInventorySnapshot
AS
BEGIN
    SET NOCOUNT ON;
    -- Full refresh for today's snapshot
    DELETE FROM silver.FactInventorySnapshot
    WHERE SnapshotDate = CAST(GETDATE() AS DATE);

    INSERT INTO silver.FactInventorySnapshot (
        ProductKey, WarehouseKey, SnapshotDate,
        UnitsOnHand, UnitsOnOrder, ReorderPoint, UnitCost)
    SELECT
        p.ProductID        AS ProductKey,
        1                  AS WarehouseKey,
        CAST(GETDATE() AS DATE) AS SnapshotDate,
        p.SafetyStockLevel AS UnitsOnHand,
        p.ReorderPoint     AS UnitsOnOrder,
        p.ReorderPoint,
        p.StandardCost     AS UnitCost
    FROM bronze.Product p;
END;

GO


-- ============================================================
-- SCENARIO: PII-rich dimension — contact info with sensitive data
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimContact
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimContact;
    INSERT INTO silver.DimContact (
        ContactAlternateKey, FirstName, LastName,
        EmailAddress, PhoneNumber, SocialSecurityNumber,
        BirthDate, StreetAddress, City, PostalCode)
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)),
        p.FirstName,
        p.LastName,
        LOWER(p.FirstName + '.' + p.LastName + '@example.com'),
        '555-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4) + '-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4),
        RIGHT('000' + CAST(c.CustomerID % 1000 AS VARCHAR), 3) + '-' + RIGHT('00' + CAST((c.CustomerID / 1000) % 100 AS VARCHAR), 2) + '-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4),
        DATEADD(DAY, -(c.CustomerID % 10000), GETDATE()),
        CAST(c.CustomerID AS NVARCHAR) + ' Main Street',
        'Anytown',
        RIGHT('00000' + CAST(c.CustomerID % 100000 AS VARCHAR), 5)
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID;
END;

GO


-- ============================================================
-- SCENARIO: accumulating snapshot — order fulfillment milestones
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactOrderFulfillment
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert new orders (OrderDate known, others NULL)
    INSERT INTO silver.FactOrderFulfillment (
        SalesOrderNumber, CustomerKey, ProductKey,
        OrderDate, OrderAmount)
    SELECT
        h.SalesOrderNumber,
        h.CustomerID,
        d.ProductID,
        h.OrderDate,
        d.LineTotal
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.FactOrderFulfillment f
        WHERE f.SalesOrderNumber = h.SalesOrderNumber);

    -- Update ship milestone
    UPDATE f
    SET f.ShipDate = h.ShipDate
    FROM silver.FactOrderFulfillment f
    JOIN bronze.SalesOrderHeader h ON f.SalesOrderNumber = h.SalesOrderNumber
    WHERE f.ShipDate IS NULL AND h.ShipDate IS NOT NULL;

    -- Update delivery milestone (estimated from ShipDate)
    UPDATE silver.FactOrderFulfillment
    SET DeliveryDate = DATEADD(DAY, 5, ShipDate)
    WHERE DeliveryDate IS NULL AND ShipDate IS NOT NULL;

    -- Update invoice milestone
    UPDATE silver.FactOrderFulfillment
    SET InvoiceDate = DeliveryDate
    WHERE InvoiceDate IS NULL AND DeliveryDate IS NOT NULL;
END;

GO


-- ============================================================
-- SCENARIO: role-playing FKs — reseller sales with 3 date keys
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactResellerSales
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.FactResellerSales;
    INSERT INTO silver.FactResellerSales (
        ProductKey, OrderDateKey, ShipDateKey, DueDateKey,
        CustomerKey, OrderQuantity, UnitPrice,
        SalesAmount, TaxAmt, Freight)
    SELECT
        d.ProductID             AS ProductKey,
        CAST(FORMAT(h.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateKey,
        CAST(FORMAT(ISNULL(h.ShipDate, h.OrderDate), 'yyyyMMdd') AS INT) AS ShipDateKey,
        CAST(FORMAT(h.DueDate, 'yyyyMMdd') AS INT)   AS DueDateKey,
        h.CustomerID            AS CustomerKey,
        d.OrderQty              AS OrderQuantity,
        d.UnitPrice,
        d.LineTotal             AS SalesAmount,
        CAST(h.TaxAmt / NULLIF(COUNT(*) OVER (PARTITION BY h.SalesOrderID), 0) AS MONEY) AS TaxAmt,
        CAST(h.Freight / NULLIF(COUNT(*) OVER (PARTITION BY h.SalesOrderID), 0) AS MONEY) AS Freight
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID;
END;

GO


-- ============================================================
-- SCENARIO: incremental load with WHERE watermark
-- ============================================================
CREATE PROCEDURE silver.usp_load_FactProductSalesDelta
    @LastLoadDate DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @LastLoadDate IS NULL
        SET @LastLoadDate = DATEADD(DAY, -1, GETDATE());

    INSERT INTO silver.FactProductSalesDelta (
        ProductKey, CustomerKey, SalesAmount, OrderDate, ModifiedDate)
    SELECT
        d.ProductID,
        h.CustomerID,
        d.LineTotal,
        h.OrderDate,
        h.ModifiedDate
    FROM bronze.SalesOrderHeader h
    JOIN bronze.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE h.ModifiedDate > @LastLoadDate;
END;

GO


-- ============================================================
-- SCENARIO: junk dimension — all flag combinations
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimSalesFlags
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimSalesFlags;
    INSERT INTO silver.DimSalesFlags (
        IsOnlineOrder, IsRushShipment, IsGiftWrapped,
        IsDiscounted, IsReturnable)
    SELECT DISTINCT
        h.OnlineOrderFlag,
        CASE WHEN h.ShipMethodID = 1 THEN 1 ELSE 0 END,
        0,
        CASE WHEN h.SubTotal <> h.TotalDue - h.TaxAmt - h.Freight THEN 1 ELSE 0 END,
        CASE WHEN h.Status IN (1, 2) THEN 1 ELSE 0 END
    FROM bronze.SalesOrderHeader h;
END;

GO


-- ============================================================
-- SCENARIO: 3+ multi-writer — three procs that all write to same target
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimMultiWriter_Full
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimMultiWriter;
    INSERT INTO silver.DimMultiWriter (
        AlternateKey, DisplayName, Category, IsActive, ModifiedDate)
    SELECT
        CAST(ProductID AS NVARCHAR(20)),
        ProductName,
        ISNULL(ProductLine, 'Unknown'),
        CASE WHEN DiscontinuedDate IS NULL THEN 1 ELSE 0 END,
        ModifiedDate
    FROM bronze.Product;
END;

GO


CREATE PROCEDURE silver.usp_load_DimMultiWriter_Delta
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimMultiWriter AS tgt
    USING (
        SELECT
            CAST(ProductID AS NVARCHAR(20)) AS AlternateKey,
            ProductName AS DisplayName,
            ISNULL(ProductLine, 'Unknown') AS Category,
            CASE WHEN DiscontinuedDate IS NULL THEN 1 ELSE 0 END AS IsActive,
            ModifiedDate
        FROM bronze.Product
    ) AS src ON tgt.AlternateKey = src.AlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.DisplayName = src.DisplayName,
        tgt.Category = src.Category,
        tgt.IsActive = src.IsActive,
        tgt.ModifiedDate = src.ModifiedDate
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        AlternateKey, DisplayName, Category, IsActive, ModifiedDate)
    VALUES (
        src.AlternateKey, src.DisplayName, src.Category, src.IsActive, src.ModifiedDate);
END;

GO


CREATE PROCEDURE silver.usp_load_DimMultiWriter_Archive
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE silver.DimMultiWriter
    SET IsActive = 0,
        ModifiedDate = GETDATE()
    WHERE AlternateKey IN (
        SELECT CAST(ProductID AS NVARCHAR(20))
        FROM bronze.Product
        WHERE DiscontinuedDate IS NOT NULL
    );
END;

GO


-- ============================================================
-- SCENARIO: IF/ELSE dynamic SQL — conditional INSERT target
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimDynamicBranch
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @mode INT;
    SELECT @mode = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 2 END
    FROM silver.DimDynamicBranch;

    IF @mode = 1
    BEGIN
        SET @sql = N'
            MERGE silver.DimDynamicBranch AS tgt
            USING (
                SELECT
                    CountryRegionCode AS BranchCode,
                    CountryRegionName AS BranchName,
                    ''Global'' AS Region,
                    GETDATE() AS LoadedAt
                FROM bronze.CountryRegion
            ) AS src ON tgt.BranchCode = src.BranchCode
            WHEN MATCHED THEN UPDATE SET
                tgt.BranchName = src.BranchName,
                tgt.LoadedAt = src.LoadedAt
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                BranchCode, BranchName, Region, LoadedAt)
            VALUES (src.BranchCode, src.BranchName, src.Region, src.LoadedAt)';
        EXEC sp_executesql @sql;
    END
    ELSE
    BEGIN
        SET @sql = N'
            INSERT INTO silver.DimDynamicBranch (BranchCode, BranchName, Region, LoadedAt)
            SELECT
                CountryRegionCode,
                CountryRegionName,
                ''Global'',
                GETDATE()
            FROM bronze.CountryRegion';
        EXEC sp_executesql @sql;
    END;
END;

GO
