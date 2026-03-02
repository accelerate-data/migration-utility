-- =============================================================================
-- WideWorldImportersDW — Staging ingestion procedures
--
-- These procedures read from WideWorldImporters (OLTP) via Integration.Get*Updates
-- and populate the memory-optimized staging tables in WideWorldImportersDW.
--
-- Pattern for all fact procedures:
--   1. Get LastCutoff from Integration.[ETL Cutoff]
--   2. Open a Lineage row (Was Successful=0)
--   3. INSERT ... EXEC into a #temp table (avoids cross-DB + memory-optimized
--      transaction restriction: msg 41317)
--   4. DELETE staging + INSERT from #temp (single-DB operation)
--   5. UPDATE ETL Cutoff + mark Lineage successful
--   CATCH: mark Lineage failed, re-throw
--
-- The #temp table must match EXACTLY the columns returned by the source proc.
-- Surrogate key columns in the staging tables (e.g. [City Key]) are NOT returned
-- by Get*Updates — they are populated later by MigrateStaged*Data procedures.
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
            [WWI City ID]                 int            NULL,
            [City]                        nvarchar(50)   NULL,
            [State Province]              nvarchar(50)   NULL,
            [Country]                     nvarchar(60)   NULL,
            [Continent]                   nvarchar(30)   NULL,
            [Sales Territory]             nvarchar(50)   NULL,
            [Region]                      nvarchar(30)   NULL,
            [Subregion]                   nvarchar(30)   NULL,
            [Location]                    geography      NULL,
            [Latest Recorded Population]  bigint         NULL,
            [Valid From]                  datetime2(7)   NULL,
            [Valid To]                    datetime2(7)   NULL
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
            [WWI Customer ID]   int            NULL,
            [Customer]          nvarchar(100)  NULL,
            [Bill To Customer]  nvarchar(100)  NULL,
            [Category]          nvarchar(50)   NULL,
            [Buying Group]      nvarchar(50)   NULL,
            [Primary Contact]   nvarchar(50)   NULL,
            [Postal Code]       nvarchar(10)   NULL,
            [Valid From]        datetime2(7)   NULL,
            [Valid To]          datetime2(7)   NULL
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
            [WWI Employee ID]   int            NULL,
            [Employee]          nvarchar(50)   NULL,
            [Preferred Name]    nvarchar(50)   NULL,
            [Is Salesperson]    bit            NULL,
            [Photo]             varbinary(max) NULL,
            [Valid From]        datetime2(7)   NULL,
            [Valid To]          datetime2(7)   NULL
        );

        INSERT INTO #EmployeeTemp
        EXEC [WideWorldImporters].Integration.GetEmployeeUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Employee_Staging;

        INSERT INTO Integration.Employee_Staging
               ([WWI Employee ID], [Employee], [Preferred Name],
                [Is Salesperson], [Photo], [Valid From], [Valid To])
        SELECT [WWI Employee ID], [Employee], [Preferred Name],
               [Is Salesperson], [Photo], [Valid From], [Valid To]
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
            [WWI Payment Method ID]  int           NULL,
            [Payment Method]         nvarchar(50)  NULL,
            [Valid From]             datetime2(7)  NULL,
            [Valid To]               datetime2(7)  NULL
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
            [WWI Stock Item ID]           int             NULL,
            [Stock Item]                  nvarchar(100)   NULL,
            [Color]                       nvarchar(20)    NULL,
            [Selling Package]             nvarchar(50)    NULL,
            [Buying Package]              nvarchar(50)    NULL,
            [Brand]                       nvarchar(50)    NULL,
            [Size]                        nvarchar(20)    NULL,
            [Lead Time Days]              int             NULL,
            [Quantity Per Outer]          int             NULL,
            [Is Chiller Stock]            bit             NULL,
            [Barcode]                     nvarchar(50)    NULL,
            [Tax Rate]                    decimal(18,3)   NULL,
            [Unit Price]                  decimal(18,2)   NULL,
            [Recommended Retail Price]    decimal(18,2)   NULL,
            [Typical Weight Per Unit]     decimal(18,3)   NULL,
            [Photo]                       varbinary(max)  NULL,
            [Valid From]                  datetime2(7)    NULL,
            [Valid To]                    datetime2(7)    NULL
        );

        INSERT INTO #StockItemTemp
        EXEC [WideWorldImporters].Integration.GetStockItemUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.StockItem_Staging;

        INSERT INTO Integration.StockItem_Staging
               ([WWI Stock Item ID], [Stock Item], [Color], [Selling Package],
                [Buying Package], [Brand], [Size], [Lead Time Days],
                [Quantity Per Outer], [Is Chiller Stock], [Barcode], [Tax Rate],
                [Unit Price], [Recommended Retail Price], [Typical Weight Per Unit],
                [Photo], [Valid From], [Valid To])
        SELECT [WWI Stock Item ID], [Stock Item], [Color], [Selling Package],
               [Buying Package], [Brand], [Size], [Lead Time Days],
               [Quantity Per Outer], [Is Chiller Stock], [Barcode], [Tax Rate],
               [Unit Price], [Recommended Retail Price], [Typical Weight Per Unit],
               [Photo], [Valid From], [Valid To]
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
            [WWI Supplier ID]       int            NULL,
            [Supplier]              nvarchar(100)  NULL,
            [Category]              nvarchar(50)   NULL,
            [Primary Contact]       nvarchar(50)   NULL,
            [Supplier Reference]    nvarchar(20)   NULL,
            [Payment Days]          int            NULL,
            [Postal Code]           nvarchar(10)   NULL,
            [Valid From]            datetime2(7)   NULL,
            [Valid To]              datetime2(7)   NULL
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
            [WWI Transaction Type ID]  int           NULL,
            [Transaction Type]         nvarchar(50)  NULL,
            [Valid From]               datetime2(7)  NULL,
            [Valid To]                 datetime2(7)  NULL
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
-- GetMovementUpdates returns 10 cols (no surrogate keys).
-- [Transaction Occurred When] from proc maps to [Last Modifed When] in staging
-- (the typo 'Modifed' is preserved from the actual staging table definition).
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
        -- Matches exactly the 10 columns returned by GetMovementUpdates.
        -- Surrogate keys ([Stock Item Key] etc.) are NOT returned by the proc;
        -- they remain NULL in staging until MigrateStagedMovementData runs.
        CREATE TABLE #MovementTemp (
            [Date Key]                      date          NULL,
            [WWI Stock Item Transaction ID] int           NULL,
            [WWI Invoice ID]                int           NULL,
            [WWI Purchase Order ID]         int           NULL,
            [Quantity]                      int           NULL,
            [WWI Stock Item ID]             int           NULL,
            [WWI Customer ID]               int           NULL,
            [WWI Supplier ID]               int           NULL,
            [WWI Transaction Type ID]       int           NULL,
            [Transaction Occurred When]     datetime2(7)  NULL
        );

        INSERT INTO #MovementTemp
        EXEC [WideWorldImporters].Integration.GetMovementUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Movement_Staging;

        INSERT INTO Integration.Movement_Staging
               ([Date Key], [WWI Stock Item Transaction ID],
                [WWI Invoice ID], [WWI Purchase Order ID], [Quantity],
                [WWI Stock Item ID], [WWI Customer ID], [WWI Supplier ID],
                [WWI Transaction Type ID], [Last Modifed When])
        SELECT [Date Key], [WWI Stock Item Transaction ID],
               [WWI Invoice ID], [WWI Purchase Order ID], [Quantity],
               [WWI Stock Item ID], [WWI Customer ID], [WWI Supplier ID],
               [WWI Transaction Type ID], [Transaction Occurred When]
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
-- GetOrderUpdates returns 18 cols (no surrogate keys).
-- [Lineage Key] is not returned by the proc; stamped via UPDATE after insert.
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
        -- Matches exactly the 18 columns returned by GetOrderUpdates.
        -- Surrogate keys ([City Key], [Customer Key], etc.) remain NULL.
        CREATE TABLE #OrderTemp (
            [Order Date Key]        date            NULL,
            [Picked Date Key]       date            NULL,
            [WWI Order ID]          int             NULL,
            [WWI Backorder ID]      int             NULL,
            [Description]           nvarchar(100)   NULL,
            [Package]               nvarchar(50)    NULL,
            [Quantity]              int             NULL,
            [Unit Price]            decimal(18,2)   NULL,
            [Tax Rate]              decimal(18,3)   NULL,
            [Total Excluding Tax]   decimal(18,2)   NULL,
            [Tax Amount]            decimal(18,2)   NULL,
            [Total Including Tax]   decimal(18,2)   NULL,
            [WWI City ID]           int             NULL,
            [WWI Customer ID]       int             NULL,
            [WWI Stock Item ID]     int             NULL,
            [WWI Salesperson ID]    int             NULL,
            [WWI Picker ID]         int             NULL,
            [Last Modified When]    datetime2(7)    NULL
        );

        INSERT INTO #OrderTemp
        EXEC [WideWorldImporters].Integration.GetOrderUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Order_Staging;

        -- [Lineage Key] excluded from INSERT; stamped below via UPDATE.
        INSERT INTO Integration.Order_Staging
               ([Order Date Key], [Picked Date Key],
                [WWI Order ID], [WWI Backorder ID], [Description], [Package], [Quantity],
                [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
                [Total Including Tax],
                [WWI City ID], [WWI Customer ID], [WWI Stock Item ID],
                [WWI Salesperson ID], [WWI Picker ID], [Last Modified When])
        SELECT [Order Date Key], [Picked Date Key],
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
-- GetPurchaseUpdates returns 10 cols (no surrogate keys).
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
        -- Matches exactly the 10 columns returned by GetPurchaseUpdates.
        -- Surrogate keys ([Supplier Key], [Stock Item Key]) remain NULL.
        CREATE TABLE #PurchaseTemp (
            [Date Key]               date           NULL,
            [WWI Purchase Order ID]  int            NULL,
            [Ordered Outers]         int            NULL,
            [Ordered Quantity]       int            NULL,
            [Received Outers]        int            NULL,
            [Package]                nvarchar(50)   NULL,
            [Is Order Finalized]     bit            NULL,
            [WWI Supplier ID]        int            NULL,
            [WWI Stock Item ID]      int            NULL,
            [Last Modified When]     datetime2(7)   NULL
        );

        INSERT INTO #PurchaseTemp
        EXEC [WideWorldImporters].Integration.GetPurchaseUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Purchase_Staging;

        INSERT INTO Integration.Purchase_Staging
               ([Date Key], [WWI Purchase Order ID], [Ordered Outers], [Ordered Quantity],
                [Received Outers], [Package], [Is Order Finalized],
                [WWI Supplier ID], [WWI Stock Item ID], [Last Modified When])
        SELECT [Date Key], [WWI Purchase Order ID], [Ordered Outers], [Ordered Quantity],
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
-- GetSaleUpdates returns 20 cols (no surrogate keys).
-- Note: the proc returns [WWI Saleperson ID] (typo in source); the #temp column
-- is named [WWI Salesperson ID] (correct). INSERT...EXEC maps by position so
-- this works correctly despite the name mismatch in the source proc.
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
        -- Matches exactly the 20 columns returned by GetSaleUpdates.
        -- Surrogate keys ([City Key], [Customer Key], etc.) remain NULL.
        CREATE TABLE #SaleTemp (
            [Invoice Date Key]          date           NULL,
            [Delivery Date Key]         date           NULL,
            [WWI Invoice ID]            int            NULL,
            [Description]               nvarchar(100)  NULL,
            [Package]                   nvarchar(50)   NULL,
            [Quantity]                  int            NULL,
            [Unit Price]                decimal(18,2)  NULL,
            [Tax Rate]                  decimal(18,3)  NULL,
            [Total Excluding Tax]       decimal(18,2)  NULL,
            [Tax Amount]                decimal(18,2)  NULL,
            [Profit]                    decimal(18,2)  NULL,
            [Total Including Tax]       decimal(18,2)  NULL,
            [Total Dry Items]           int            NULL,
            [Total Chiller Items]       int            NULL,
            [WWI City ID]               int            NULL,
            [WWI Customer ID]           int            NULL,
            [WWI Bill To Customer ID]   int            NULL,
            [WWI Stock Item ID]         int            NULL,
            [WWI Salesperson ID]        int            NULL,
            [Last Modified When]        datetime2(7)   NULL
        );

        INSERT INTO #SaleTemp
        EXEC [WideWorldImporters].Integration.GetSaleUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Sale_Staging;

        INSERT INTO Integration.Sale_Staging
               ([Invoice Date Key], [Delivery Date Key], [WWI Invoice ID],
                [Description], [Package], [Quantity],
                [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
                [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items],
                [WWI City ID], [WWI Customer ID], [WWI Bill To Customer ID],
                [WWI Stock Item ID], [WWI Salesperson ID], [Last Modified When])
        SELECT [Invoice Date Key], [Delivery Date Key], [WWI Invoice ID],
               [Description], [Package], [Quantity],
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
-- GetStockHoldingUpdates returns 7 cols (no date filter — full snapshot).
-- Surrogate key [Stock Item Key] remains NULL.
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
        -- Matches exactly the 7 columns returned by GetStockHoldingUpdates.
        -- [Stock Item Key] surrogate remains NULL.
        CREATE TABLE #StockHoldingTemp (
            [Quantity On Hand]        int            NULL,
            [Bin Location]            nvarchar(20)   NULL,
            [Last Stocktake Quantity] int            NULL,
            [Last Cost Price]         decimal(18,2)  NULL,
            [Reorder Level]           int            NULL,
            [Target Stock Level]      int            NULL,
            [WWI Stock Item ID]       int            NULL
        );

        INSERT INTO #StockHoldingTemp
        EXEC [WideWorldImporters].Integration.GetStockHoldingUpdates;

        DELETE FROM Integration.StockHolding_Staging;

        INSERT INTO Integration.StockHolding_Staging
               ([Quantity On Hand], [Bin Location], [Last Stocktake Quantity],
                [Last Cost Price], [Reorder Level], [Target Stock Level], [WWI Stock Item ID])
        SELECT [Quantity On Hand], [Bin Location], [Last Stocktake Quantity],
               [Last Cost Price], [Reorder Level], [Target Stock Level], [WWI Stock Item ID]
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
-- GetTransactionUpdates returns 17 cols (UNION of customer + supplier txns).
-- Surrogate keys ([Customer Key], [Bill To Customer Key], etc.) remain NULL.
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
        -- Matches exactly the 17 columns returned by GetTransactionUpdates.
        -- Surrogate keys remain NULL.
        CREATE TABLE #TransactionTemp (
            [Date Key]                      date           NULL,
            [WWI Customer Transaction ID]   int            NULL,
            [WWI Supplier Transaction ID]   int            NULL,
            [WWI Invoice ID]                int            NULL,
            [WWI Purchase Order ID]         int            NULL,
            [Supplier Invoice Number]       nvarchar(20)   NULL,
            [Total Excluding Tax]           decimal(18,2)  NULL,
            [Tax Amount]                    decimal(18,2)  NULL,
            [Total Including Tax]           decimal(18,2)  NULL,
            [Outstanding Balance]           decimal(18,2)  NULL,
            [Is Finalized]                  bit            NULL,
            [WWI Customer ID]               int            NULL,
            [WWI Bill To Customer ID]       int            NULL,
            [WWI Supplier ID]               int            NULL,
            [WWI Transaction Type ID]       int            NULL,
            [WWI Payment Method ID]         int            NULL,
            [Last Modified When]            datetime2(7)   NULL
        );

        INSERT INTO #TransactionTemp
        EXEC [WideWorldImporters].Integration.GetTransactionUpdates @LastCutoff, @NewCutoff;

        DELETE FROM Integration.Transaction_Staging;

        INSERT INTO Integration.Transaction_Staging
               ([Date Key],
                [WWI Customer Transaction ID], [WWI Supplier Transaction ID],
                [WWI Invoice ID], [WWI Purchase Order ID], [Supplier Invoice Number],
                [Total Excluding Tax], [Tax Amount], [Total Including Tax],
                [Outstanding Balance], [Is Finalized],
                [WWI Customer ID], [WWI Bill To Customer ID], [WWI Supplier ID],
                [WWI Transaction Type ID], [WWI Payment Method ID], [Last Modified When])
        SELECT [Date Key],
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

-- =============================================================================
-- Reset ETL Cutoff times to force a full reload on next run.
-- Run this once after deploying corrected procedures.
-- =============================================================================
UPDATE Integration.[ETL Cutoff]
SET    [Cutoff Time] = CONVERT(datetime2(7), '19000101', 112);
GO
