-- =============================================================================
-- scripts/sql/wwi-ingest-staging.sql
--
-- Creates Integration.Ingest*Staging stored procedures in WideWorldImportersDW.
-- Each procedure performs a full refresh of one Integration.*_Staging table
-- by calling the corresponding Integration.Get*Updates procedure in
-- WideWorldImporters (OLTP) on the same SQL Server instance.
--
-- Lineage and ETL Cutoff are updated on each run (success or failure).
-- Column lists are sourced from WideWorldImportersDW v1.0 staging table schemas.
--
-- Usage:
--   sqlcmd -S <host>,<port> -U sa -P <password> -C \
--          -i scripts/sql/wwi-ingest-staging.sql
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
        DELETE FROM Integration.City_Staging;

        INSERT INTO Integration.City_Staging
               ([WWI City ID], [City], [State Province], [Country], [Continent],
                [Sales Territory], [Region], [Subregion], [Location],
                [Latest Recorded Population], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetCityUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'City';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.Customer_Staging;

        INSERT INTO Integration.Customer_Staging
               ([WWI Customer ID], [Customer], [Bill To Customer], [Category],
                [Buying Group], [Primary Contact], [Postal Code], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetCustomerUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Customer';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.Employee_Staging;

        INSERT INTO Integration.Employee_Staging
               ([WWI Employee ID], [Employee], [Preferred Name], [Is Salesperson],
                [Photo], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetEmployeeUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Employee';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.PaymentMethod_Staging;

        INSERT INTO Integration.PaymentMethod_Staging
               ([WWI Payment Method ID], [Payment Method], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetPaymentMethodUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Payment Method';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.StockItem_Staging;

        INSERT INTO Integration.StockItem_Staging
               ([WWI Stock Item ID], [Stock Item], [Color], [Selling Package], [Buying Package],
                [Brand], [Size], [Lead Time Days], [Quantity Per Outer], [Is Chiller Stock],
                [Barcode], [Tax Rate], [Unit Price], [Recommended Retail Price],
                [Typical Weight Per Unit], [Photo], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetStockItemUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Stock Item';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.Supplier_Staging;

        INSERT INTO Integration.Supplier_Staging
               ([WWI Supplier ID], [Supplier], [Category], [Primary Contact],
                [Supplier Reference], [Payment Days], [Postal Code], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetSupplierUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Supplier';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.TransactionType_Staging;

        INSERT INTO Integration.TransactionType_Staging
               ([WWI Transaction Type ID], [Transaction Type], [Valid From], [Valid To])
        EXEC [WideWorldImporters].Integration.GetTransactionTypeUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Transaction Type';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.Movement_Staging;

        INSERT INTO Integration.Movement_Staging
               ([Date Key], [Stock Item Key], [Customer Key], [Supplier Key],
                [Transaction Type Key], [WWI Stock Item Transaction ID],
                [WWI Invoice ID], [WWI Purchase Order ID], [Quantity],
                [WWI Stock Item ID], [WWI Customer ID], [WWI Supplier ID],
                [WWI Transaction Type ID], [Last Modifed When])
        EXEC [WideWorldImporters].Integration.GetMovementUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Movement';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
-- Note: Order_Staging has a [Lineage Key] column not returned by GetOrderUpdates.
--       It is stamped onto all inserted rows after the bulk load.
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
        EXEC [WideWorldImporters].Integration.GetOrderUpdates @LastCutoff, @NewCutoff;

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
        DELETE FROM Integration.Purchase_Staging;

        INSERT INTO Integration.Purchase_Staging
               ([Date Key], [Supplier Key], [Stock Item Key],
                [WWI Purchase Order ID], [Ordered Outers], [Ordered Quantity],
                [Received Outers], [Package], [Is Order Finalized],
                [WWI Supplier ID], [WWI Stock Item ID], [Last Modified When])
        EXEC [WideWorldImporters].Integration.GetPurchaseUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Purchase';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.Sale_Staging;

        INSERT INTO Integration.Sale_Staging
               ([City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key],
                [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
                [WWI Invoice ID], [Description], [Package], [Quantity],
                [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
                [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items],
                [WWI City ID], [WWI Customer ID], [WWI Bill To Customer ID],
                [WWI Stock Item ID], [WWI Salesperson ID], [Last Modified When])
        EXEC [WideWorldImporters].Integration.GetSaleUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Sale';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        DELETE FROM Integration.StockHolding_Staging;

        INSERT INTO Integration.StockHolding_Staging
               ([Stock Item Key], [Quantity On Hand], [Bin Location],
                [Last Stocktake Quantity], [Last Cost Price],
                [Reorder Level], [Target Stock Level], [WWI Stock Item ID])
        EXEC [WideWorldImporters].Integration.GetStockHoldingUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Stock Holding';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
        EXEC [WideWorldImporters].Integration.GetTransactionUpdates @LastCutoff, @NewCutoff;

        UPDATE Integration.[ETL Cutoff]
        SET    [Cutoff Time] = @NewCutoff
        WHERE  [Table Name]  = N'Transaction';

        UPDATE Integration.Lineage
        SET    [Data Load Completed] = SYSDATETIME(),
               [Was Successful]      = 1
        WHERE  [Lineage Key] = @LineageKey;
    END TRY
    BEGIN CATCH
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
