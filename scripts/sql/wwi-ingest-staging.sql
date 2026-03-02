-- =============================================================================
-- scripts/sql/wwi-ingest-staging.sql
--
-- Creates Integration.Ingest*Staging stored procedures in WideWorldImportersDW.
-- Each procedure performs a full refresh of one Integration.*_Staging table
-- by calling the corresponding Integration.Get*Updates procedure in
-- WideWorldImporters (OLTP) on the same SQL Server instance.
--
-- Architecture note: WideWorldImportersDW staging tables are memory-optimized
-- (SCHEMA_ONLY). Memory-optimized tables cannot participate in cross-database
-- transactions, so each procedure stages data through a regular #temp table:
--   1. INSERT INTO #temp EXEC [WideWorldImporters].Integration.Get*Updates
--   2. DELETE FROM memory-optimized staging table
--   3. INSERT INTO staging SELECT FROM #temp
--
-- Usage:
--   docker cp scripts/sql/wwi-ingest-staging.sql aw-sql:/tmp/wwi-ingest-staging.sql
--   docker exec aw-sql /opt/mssql-tools18/bin/sqlcmd \
--     -S localhost,1433 -U sa -P '<password>' -C \
--     -i /tmp/wwi-ingest-staging.sql
--
--   Ingest all tables:  EXEC Integration.IngestAllStaging;
--   Ingest one table:   EXEC Integration.IngestCityStaging;
-- =============================================================================

USE [WideWorldImportersDW];
GO

-- =============================================================================
-- DIMENSIONS (7)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- City  |  ETL Cutoff: 'City'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestCityStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'City';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'City', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #CityTemp (
            [WWI City ID]                int            NOT NULL,
            [City]                       nvarchar(50)   NOT NULL,
            [State Province]             nvarchar(50)   NOT NULL,
            [Country]                    nvarchar(60)   NOT NULL,
            [Continent]                  nvarchar(30)   NOT NULL,
            [Sales Territory]            nvarchar(50)   NOT NULL,
            [Region]                     nvarchar(30)   NOT NULL,
            [Subregion]                  nvarchar(30)   NOT NULL,
            [Location]                   geography          NULL,
            [Latest Recorded Population] bigint             NULL,
            [Valid From]                 datetime2(7)   NOT NULL,
            [Valid To]                   datetime2(7)   NOT NULL
        );

        INSERT INTO #CityTemp
        EXEC [WideWorldImporters].Integration.GetCityUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.City_Staging;

        INSERT INTO Integration.City_Staging
               ([WWI City ID], [City], [State Province], [Country], [Continent],
                [Sales Territory], [Region], [Subregion], [Location],
                [Latest Recorded Population], [Valid From], [Valid To])
        SELECT [WWI City ID], [City], [State Province], [Country], [Continent],
               [Sales Territory], [Region], [Subregion], [Location],
               [Latest Recorded Population], [Valid From], [Valid To]
        FROM   #CityTemp;

        DROP TABLE #CityTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'City';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#CityTemp') IS NOT NULL DROP TABLE #CityTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Customer  |  ETL Cutoff: 'Customer'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestCustomerStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Customer';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Customer', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #CustomerTemp (
            [WWI Customer ID]   int            NOT NULL,
            [Customer]          nvarchar(100)  NOT NULL,
            [Bill To Customer]  nvarchar(100)  NOT NULL,
            [Category]          nvarchar(50)   NOT NULL,
            [Buying Group]      nvarchar(50)   NOT NULL,
            [Primary Contact]   nvarchar(50)   NOT NULL,
            [Postal Code]       nvarchar(10)   NOT NULL,
            [Valid From]        datetime2(7)   NOT NULL,
            [Valid To]          datetime2(7)   NOT NULL
        );

        INSERT INTO #CustomerTemp
        EXEC [WideWorldImporters].Integration.GetCustomerUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Customer_Staging;

        INSERT INTO Integration.Customer_Staging
               ([WWI Customer ID], [Customer], [Bill To Customer], [Category],
                [Buying Group], [Primary Contact], [Postal Code], [Valid From], [Valid To])
        SELECT [WWI Customer ID], [Customer], [Bill To Customer], [Category],
               [Buying Group], [Primary Contact], [Postal Code], [Valid From], [Valid To]
        FROM   #CustomerTemp;

        DROP TABLE #CustomerTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Customer';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#CustomerTemp') IS NOT NULL DROP TABLE #CustomerTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Employee  |  ETL Cutoff: 'Employee'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestEmployeeStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Employee';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Employee', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #EmployeeTemp (
            [WWI Employee ID]  int            NOT NULL,
            [Employee]         nvarchar(50)   NOT NULL,
            [Preferred Name]   nvarchar(50)   NOT NULL,
            [Is Salesperson]   bit            NOT NULL,
            [Photo]            varbinary(max)     NULL,
            [Valid From]       datetime2(7)   NOT NULL,
            [Valid To]         datetime2(7)   NOT NULL
        );

        INSERT INTO #EmployeeTemp
        EXEC [WideWorldImporters].Integration.GetEmployeeUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Employee_Staging;

        INSERT INTO Integration.Employee_Staging
               ([WWI Employee ID], [Employee], [Preferred Name], [Is Salesperson],
                [Photo], [Valid From], [Valid To])
        SELECT [WWI Employee ID], [Employee], [Preferred Name], [Is Salesperson],
               [Photo], [Valid From], [Valid To]
        FROM   #EmployeeTemp;

        DROP TABLE #EmployeeTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Employee';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#EmployeeTemp') IS NOT NULL DROP TABLE #EmployeeTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Payment Method  |  ETL Cutoff: 'Payment Method'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestPaymentMethodStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Payment Method';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Payment Method', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #PaymentMethodTemp (
            [WWI Payment Method ID] int           NOT NULL,
            [Payment Method]        nvarchar(50)  NOT NULL,
            [Valid From]            datetime2(7)  NOT NULL,
            [Valid To]              datetime2(7)  NOT NULL
        );

        INSERT INTO #PaymentMethodTemp
        EXEC [WideWorldImporters].Integration.GetPaymentMethodUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.PaymentMethod_Staging;

        INSERT INTO Integration.PaymentMethod_Staging
               ([WWI Payment Method ID], [Payment Method], [Valid From], [Valid To])
        SELECT [WWI Payment Method ID], [Payment Method], [Valid From], [Valid To]
        FROM   #PaymentMethodTemp;

        DROP TABLE #PaymentMethodTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Payment Method';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#PaymentMethodTemp') IS NOT NULL DROP TABLE #PaymentMethodTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Stock Item  |  ETL Cutoff: 'Stock Item'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestStockItemStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Stock Item';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Stock Item', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #StockItemTemp (
            [WWI Stock Item ID]          int             NOT NULL,
            [Stock Item]                 nvarchar(100)   NOT NULL,
            [Color]                      nvarchar(20)    NOT NULL,
            [Selling Package]            nvarchar(50)    NOT NULL,
            [Buying Package]             nvarchar(50)    NOT NULL,
            [Brand]                      nvarchar(50)    NOT NULL,
            [Size]                       nvarchar(20)    NOT NULL,
            [Lead Time Days]             int             NOT NULL,
            [Quantity Per Outer]         int             NOT NULL,
            [Is Chiller Stock]           bit             NOT NULL,
            [Barcode]                    nvarchar(50)        NULL,
            [Tax Rate]                   decimal(18,3)   NOT NULL,
            [Unit Price]                 decimal(18,2)   NOT NULL,
            [Recommended Retail Price]   decimal(18,2)       NULL,
            [Typical Weight Per Unit]    decimal(18,3)   NOT NULL,
            [Photo]                      varbinary(max)      NULL,
            [Valid From]                 datetime2(7)    NOT NULL,
            [Valid To]                   datetime2(7)    NOT NULL
        );

        INSERT INTO #StockItemTemp
        EXEC [WideWorldImporters].Integration.GetStockItemUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.StockItem_Staging;

        INSERT INTO Integration.StockItem_Staging
               ([WWI Stock Item ID], [Stock Item], [Color], [Selling Package], [Buying Package],
                [Brand], [Size], [Lead Time Days], [Quantity Per Outer], [Is Chiller Stock],
                [Barcode], [Tax Rate], [Unit Price], [Recommended Retail Price],
                [Typical Weight Per Unit], [Photo], [Valid From], [Valid To])
        SELECT [WWI Stock Item ID], [Stock Item], [Color], [Selling Package], [Buying Package],
               [Brand], [Size], [Lead Time Days], [Quantity Per Outer], [Is Chiller Stock],
               [Barcode], [Tax Rate], [Unit Price], [Recommended Retail Price],
               [Typical Weight Per Unit], [Photo], [Valid From], [Valid To]
        FROM   #StockItemTemp;

        DROP TABLE #StockItemTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Stock Item';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#StockItemTemp') IS NOT NULL DROP TABLE #StockItemTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Supplier  |  ETL Cutoff: 'Supplier'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestSupplierStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Supplier';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Supplier', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #SupplierTemp (
            [WWI Supplier ID]    int            NOT NULL,
            [Supplier]           nvarchar(100)  NOT NULL,
            [Category]           nvarchar(50)   NOT NULL,
            [Primary Contact]    nvarchar(50)   NOT NULL,
            [Supplier Reference] nvarchar(20)       NULL,
            [Payment Days]       int            NOT NULL,
            [Postal Code]        nvarchar(10)   NOT NULL,
            [Valid From]         datetime2(7)   NOT NULL,
            [Valid To]           datetime2(7)   NOT NULL
        );

        INSERT INTO #SupplierTemp
        EXEC [WideWorldImporters].Integration.GetSupplierUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Supplier_Staging;

        INSERT INTO Integration.Supplier_Staging
               ([WWI Supplier ID], [Supplier], [Category], [Primary Contact],
                [Supplier Reference], [Payment Days], [Postal Code], [Valid From], [Valid To])
        SELECT [WWI Supplier ID], [Supplier], [Category], [Primary Contact],
               [Supplier Reference], [Payment Days], [Postal Code], [Valid From], [Valid To]
        FROM   #SupplierTemp;

        DROP TABLE #SupplierTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Supplier';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#SupplierTemp') IS NOT NULL DROP TABLE #SupplierTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Transaction Type  |  ETL Cutoff: 'Transaction Type'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestTransactionTypeStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Transaction Type';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Transaction Type', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #TransactionTypeTemp (
            [WWI Transaction Type ID] int           NOT NULL,
            [Transaction Type]        nvarchar(50)  NOT NULL,
            [Valid From]              datetime2(7)  NOT NULL,
            [Valid To]                datetime2(7)  NOT NULL
        );

        INSERT INTO #TransactionTypeTemp
        EXEC [WideWorldImporters].Integration.GetTransactionTypeUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.TransactionType_Staging;

        INSERT INTO Integration.TransactionType_Staging
               ([WWI Transaction Type ID], [Transaction Type], [Valid From], [Valid To])
        SELECT [WWI Transaction Type ID], [Transaction Type], [Valid From], [Valid To]
        FROM   #TransactionTypeTemp;

        DROP TABLE #TransactionTypeTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Transaction Type';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#TransactionTypeTemp') IS NOT NULL DROP TABLE #TransactionTypeTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- =============================================================================
-- FACTS (6)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Movement  |  ETL Cutoff: 'Movement'
-- Note: [Last Modifed When] preserves the typo present in the staging table.
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestMovementStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Movement';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Movement', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #MovementTemp (
            [Date Key]                      date          NOT NULL,
            [Stock Item Key]                int           NOT NULL,
            [Customer Key]                  int               NULL,
            [Supplier Key]                  int               NULL,
            [Transaction Type Key]          int           NOT NULL,
            [WWI Stock Item Transaction ID] int           NOT NULL,
            [WWI Invoice ID]                int               NULL,
            [WWI Purchase Order ID]         int               NULL,
            [Quantity]                      int           NOT NULL,
            [WWI Stock Item ID]             int           NOT NULL,
            [WWI Customer ID]               int               NULL,
            [WWI Supplier ID]               int               NULL,
            [WWI Transaction Type ID]       int           NOT NULL,
            [Last Modifed When]             datetime2(7)  NOT NULL
        );

        INSERT INTO #MovementTemp
        EXEC [WideWorldImporters].Integration.GetMovementUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Movement_Staging;

        INSERT INTO Integration.Movement_Staging
               ([Date Key], [Stock Item Key], [Customer Key], [Supplier Key],
                [Transaction Type Key], [WWI Stock Item Transaction ID],
                [WWI Invoice ID], [WWI Purchase Order ID], [Quantity],
                [WWI Stock Item ID], [WWI Customer ID], [WWI Supplier ID],
                [WWI Transaction Type ID], [Last Modifed When])
        SELECT [Date Key], [Stock Item Key], [Customer Key], [Supplier Key],
               [Transaction Type Key], [WWI Stock Item Transaction ID],
               [WWI Invoice ID], [WWI Purchase Order ID], [Quantity],
               [WWI Stock Item ID], [WWI Customer ID], [WWI Supplier ID],
               [WWI Transaction Type ID], [Last Modifed When]
        FROM   #MovementTemp;

        DROP TABLE #MovementTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Movement';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#MovementTemp') IS NOT NULL DROP TABLE #MovementTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Order  |  ETL Cutoff: 'Order'
-- Note: [Lineage Key] is not returned by GetOrderUpdates; stamped after insert.
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestOrderStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Order';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Order', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #OrderTemp (
            [City Key]              int             NOT NULL,
            [Customer Key]          int             NOT NULL,
            [Stock Item Key]        int             NOT NULL,
            [Order Date Key]        date            NOT NULL,
            [Picked Date Key]       date                NULL,
            [Salesperson Key]       int             NOT NULL,
            [Picker Key]            int                 NULL,
            [WWI Order ID]          int             NOT NULL,
            [WWI Backorder ID]      int                 NULL,
            [Description]           nvarchar(100)   NOT NULL,
            [Package]               nvarchar(50)    NOT NULL,
            [Quantity]              int             NOT NULL,
            [Unit Price]            decimal(18,2)   NOT NULL,
            [Tax Rate]              decimal(18,3)   NOT NULL,
            [Total Excluding Tax]   decimal(18,2)   NOT NULL,
            [Tax Amount]            decimal(18,2)   NOT NULL,
            [Total Including Tax]   decimal(18,2)   NOT NULL,
            [WWI City ID]           int             NOT NULL,
            [WWI Customer ID]       int             NOT NULL,
            [WWI Stock Item ID]     int             NOT NULL,
            [WWI Salesperson ID]    int             NOT NULL,
            [WWI Picker ID]         int                 NULL,
            [Last Modified When]    datetime2(7)    NOT NULL
        );

        INSERT INTO #OrderTemp
        EXEC [WideWorldImporters].Integration.GetOrderUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Order_Staging;

        -- [Lineage Key] excluded from INSERT; stamped below.
        INSERT INTO Integration.Order_Staging
               ([City Key], [Customer Key], [Stock Item Key],
                [Order Date Key], [Picked Date Key], [Salesperson Key], [Picker Key],
                [WWI Order ID], [WWI Backorder ID], [Description], [Package], [Quantity],
                [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
                [Total Including Tax],
                [WWI City ID], [WWI Customer ID], [WWI Stock Item ID],
                [WWI Salesperson ID], [WWI Picker ID], [Last Modified When])
        SELECT [City Key], [Customer Key], [Stock Item Key],
               [Order Date Key], [Picked Date Key], [Salesperson Key], [Picker Key],
               [WWI Order ID], [WWI Backorder ID], [Description], [Package], [Quantity],
               [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
               [Total Including Tax],
               [WWI City ID], [WWI Customer ID], [WWI Stock Item ID],
               [WWI Salesperson ID], [WWI Picker ID], [Last Modified When]
        FROM   #OrderTemp;

        DROP TABLE #OrderTemp;

        UPDATE Integration.Order_Staging
        SET    [Lineage Key] = @LineageKey;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Order';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#OrderTemp') IS NOT NULL DROP TABLE #OrderTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Purchase  |  ETL Cutoff: 'Purchase'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestPurchaseStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Purchase';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Purchase', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #PurchaseTemp (
            [Date Key]               date           NOT NULL,
            [Supplier Key]           int            NOT NULL,
            [Stock Item Key]         int            NOT NULL,
            [WWI Purchase Order ID]  int            NOT NULL,
            [Ordered Outers]         int            NOT NULL,
            [Ordered Quantity]       int            NOT NULL,
            [Received Outers]        int            NOT NULL,
            [Package]                nvarchar(50)   NOT NULL,
            [Is Order Finalized]     bit            NOT NULL,
            [WWI Supplier ID]        int            NOT NULL,
            [WWI Stock Item ID]      int            NOT NULL,
            [Last Modified When]     datetime2(7)   NOT NULL
        );

        INSERT INTO #PurchaseTemp
        EXEC [WideWorldImporters].Integration.GetPurchaseUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Purchase_Staging;

        INSERT INTO Integration.Purchase_Staging
               ([Date Key], [Supplier Key], [Stock Item Key],
                [WWI Purchase Order ID], [Ordered Outers], [Ordered Quantity],
                [Received Outers], [Package], [Is Order Finalized],
                [WWI Supplier ID], [WWI Stock Item ID], [Last Modified When])
        SELECT [Date Key], [Supplier Key], [Stock Item Key],
               [WWI Purchase Order ID], [Ordered Outers], [Ordered Quantity],
               [Received Outers], [Package], [Is Order Finalized],
               [WWI Supplier ID], [WWI Stock Item ID], [Last Modified When]
        FROM   #PurchaseTemp;

        DROP TABLE #PurchaseTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Purchase';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#PurchaseTemp') IS NOT NULL DROP TABLE #PurchaseTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Sale  |  ETL Cutoff: 'Sale'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestSaleStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Sale';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Sale', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #SaleTemp (
            [City Key]              int            NOT NULL,
            [Customer Key]          int            NOT NULL,
            [Bill To Customer Key]  int            NOT NULL,
            [Stock Item Key]        int            NOT NULL,
            [Invoice Date Key]      date           NOT NULL,
            [Delivery Date Key]     date               NULL,
            [Salesperson Key]       int            NOT NULL,
            [WWI Invoice ID]        int            NOT NULL,
            [Description]           nvarchar(100)  NOT NULL,
            [Package]               nvarchar(50)   NOT NULL,
            [Quantity]              int            NOT NULL,
            [Unit Price]            decimal(18,2)  NOT NULL,
            [Tax Rate]              decimal(18,3)  NOT NULL,
            [Total Excluding Tax]   decimal(18,2)  NOT NULL,
            [Tax Amount]            decimal(18,2)  NOT NULL,
            [Profit]                decimal(18,2)  NOT NULL,
            [Total Including Tax]   decimal(18,2)  NOT NULL,
            [Total Dry Items]       int            NOT NULL,
            [Total Chiller Items]   int            NOT NULL,
            [WWI City ID]           int            NOT NULL,
            [WWI Customer ID]       int            NOT NULL,
            [WWI Bill To Customer ID] int          NOT NULL,
            [WWI Stock Item ID]     int            NOT NULL,
            [WWI Salesperson ID]    int            NOT NULL,
            [Last Modified When]    datetime2(7)   NOT NULL
        );

        INSERT INTO #SaleTemp
        EXEC [WideWorldImporters].Integration.GetSaleUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Sale_Staging;

        INSERT INTO Integration.Sale_Staging
               ([City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key],
                [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
                [WWI Invoice ID], [Description], [Package], [Quantity],
                [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
                [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items],
                [WWI City ID], [WWI Customer ID], [WWI Bill To Customer ID],
                [WWI Stock Item ID], [WWI Salesperson ID], [Last Modified When])
        SELECT [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key],
               [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
               [WWI Invoice ID], [Description], [Package], [Quantity],
               [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
               [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items],
               [WWI City ID], [WWI Customer ID], [WWI Bill To Customer ID],
               [WWI Stock Item ID], [WWI Salesperson ID], [Last Modified When]
        FROM   #SaleTemp;

        DROP TABLE #SaleTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Sale';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#SaleTemp') IS NOT NULL DROP TABLE #SaleTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Stock Holding  |  ETL Cutoff: 'Stock Holding'
-- Full refresh — no temporal filter on source data.
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestStockHoldingStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Stock Holding';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Stock Holding', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #StockHoldingTemp (
            [Stock Item Key]         int            NOT NULL,
            [Quantity On Hand]       int            NOT NULL,
            [Bin Location]           nvarchar(20)   NOT NULL,
            [Last Stocktake Quantity] int           NOT NULL,
            [Last Cost Price]        decimal(18,2)  NOT NULL,
            [Reorder Level]          int            NOT NULL,
            [Target Stock Level]     int            NOT NULL,
            [WWI Stock Item ID]      int            NOT NULL
        );

        INSERT INTO #StockHoldingTemp
        EXEC [WideWorldImporters].Integration.GetStockHoldingUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.StockHolding_Staging;

        INSERT INTO Integration.StockHolding_Staging
               ([Stock Item Key], [Quantity On Hand], [Bin Location],
                [Last Stocktake Quantity], [Last Cost Price],
                [Reorder Level], [Target Stock Level], [WWI Stock Item ID])
        SELECT [Stock Item Key], [Quantity On Hand], [Bin Location],
               [Last Stocktake Quantity], [Last Cost Price],
               [Reorder Level], [Target Stock Level], [WWI Stock Item ID]
        FROM   #StockHoldingTemp;

        DROP TABLE #StockHoldingTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Stock Holding';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#StockHoldingTemp') IS NOT NULL DROP TABLE #StockHoldingTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- -----------------------------------------------------------------------------
-- Transaction  |  ETL Cutoff: 'Transaction'
-- -----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE Integration.IngestTransactionStaging
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LineageKey int;
    DECLARE @LastCutoff datetime2(7);
    DECLARE @NewCutoff  datetime2(7) = SYSDATETIME();

    SELECT @LastCutoff = [Cutoff Time]
    FROM   Integration.[ETL Cutoff]
    WHERE  [Table Name] = N'Transaction';

    IF @LastCutoff IS NULL
        SET @LastCutoff = CONVERT(datetime2(7), '19000101', 112);

    INSERT INTO Integration.Lineage
           ([Data Load Started], [Table Name], [Was Successful], [Source System Cutoff Time])
    VALUES (@NewCutoff, N'Transaction', 0, @NewCutoff);
    SET @LineageKey = SCOPE_IDENTITY();

    BEGIN TRY
        CREATE TABLE #TransactionTemp (
            [Date Key]                      date           NOT NULL,
            [Customer Key]                  int                NULL,
            [Bill To Customer Key]          int                NULL,
            [Supplier Key]                  int                NULL,
            [Transaction Type Key]          int            NOT NULL,
            [Payment Method Key]            int                NULL,
            [WWI Customer Transaction ID]   int                NULL,
            [WWI Supplier Transaction ID]   int                NULL,
            [WWI Invoice ID]                int                NULL,
            [WWI Purchase Order ID]         int                NULL,
            [Supplier Invoice Number]       nvarchar(20)       NULL,
            [Total Excluding Tax]           decimal(18,2)  NOT NULL,
            [Tax Amount]                    decimal(18,2)  NOT NULL,
            [Total Including Tax]           decimal(18,2)  NOT NULL,
            [Outstanding Balance]           decimal(18,2)  NOT NULL,
            [Is Finalized]                  bit            NOT NULL,
            [WWI Customer ID]               int                NULL,
            [WWI Bill To Customer ID]       int                NULL,
            [WWI Supplier ID]               int                NULL,
            [WWI Transaction Type ID]       int            NOT NULL,
            [WWI Payment Method ID]         int                NULL,
            [Last Modified When]            datetime2(7)   NOT NULL
        );

        INSERT INTO #TransactionTemp
        EXEC [WideWorldImporters].Integration.GetTransactionUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Transaction_Staging;

        INSERT INTO Integration.Transaction_Staging
               ([Date Key], [Customer Key], [Bill To Customer Key], [Supplier Key],
                [Transaction Type Key], [Payment Method Key],
                [WWI Customer Transaction ID], [WWI Supplier Transaction ID],
                [WWI Invoice ID], [WWI Purchase Order ID], [Supplier Invoice Number],
                [Total Excluding Tax], [Tax Amount], [Total Including Tax],
                [Outstanding Balance], [Is Finalized],
                [WWI Customer ID], [WWI Bill To Customer ID], [WWI Supplier ID],
                [WWI Transaction Type ID], [WWI Payment Method ID], [Last Modified When])
        SELECT [Date Key], [Customer Key], [Bill To Customer Key], [Supplier Key],
               [Transaction Type Key], [Payment Method Key],
               [WWI Customer Transaction ID], [WWI Supplier Transaction ID],
               [WWI Invoice ID], [WWI Purchase Order ID], [Supplier Invoice Number],
               [Total Excluding Tax], [Tax Amount], [Total Including Tax],
               [Outstanding Balance], [Is Finalized],
               [WWI Customer ID], [WWI Bill To Customer ID], [WWI Supplier ID],
               [WWI Transaction Type ID], [WWI Payment Method ID], [Last Modified When]
        FROM   #TransactionTemp;

        DROP TABLE #TransactionTemp;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Transaction';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#TransactionTemp') IS NOT NULL DROP TABLE #TransactionTemp;
        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 0
        WHERE  [Lineage Key] = @LineageKey;
        THROW;
    END CATCH;
END;
GO

-- =============================================================================
-- MASTER: run all 13 in dependency order (dimensions first, then facts)
-- =============================================================================
CREATE OR ALTER PROCEDURE Integration.IngestAllStaging
AS
BEGIN
    SET NOCOUNT ON;

    EXEC Integration.IngestCityStaging;
    EXEC Integration.IngestCustomerStaging;
    EXEC Integration.IngestEmployeeStaging;
    EXEC Integration.IngestPaymentMethodStaging;
    EXEC Integration.IngestStockItemStaging;
    EXEC Integration.IngestSupplierStaging;
    EXEC Integration.IngestTransactionTypeStaging;
    EXEC Integration.IngestMovementStaging;
    EXEC Integration.IngestOrderStaging;
    EXEC Integration.IngestPurchaseStaging;
    EXEC Integration.IngestSaleStaging;
    EXEC Integration.IngestStockHoldingStaging;
    EXEC Integration.IngestTransactionStaging;
END;
GO
