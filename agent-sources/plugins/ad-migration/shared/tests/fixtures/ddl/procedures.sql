-- Scenario 1: single INSERT+SELECT — sqlglot parses as Create, refs extractable
-- Expected: writes_to=[silver.dimproduct], reads_from=[bronze.product], calls=[], parse_error=null
CREATE PROCEDURE [dbo].[usp_simple_insert]
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey, EnglishProductName)
    SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product
END
GO
-- Scenario 2: nested IF/ELSE with BEGIN/END — sqlglot falls back to top-level Command
-- Expected: parse_error set, refs empty
CREATE PROCEDURE [dbo].[usp_nested_if_else]
    @Mode INT = 0
AS
BEGIN
    IF @Mode = 1
    BEGIN
        INSERT INTO silver.DimProduct (ProductAlternateKey) SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product
    END
    ELSE
    BEGIN
        UPDATE silver.DimProduct SET EnglishProductName = 'Unknown' WHERE ProductKey = 0
    END
END
GO
-- Scenario 3: MERGE — sqlglot falls back to top-level Command
-- Expected: parse_error set, refs empty
CREATE PROCEDURE [dbo].[usp_merge]
AS
BEGIN
    MERGE silver.DimProduct AS tgt
    USING (SELECT ProductID, ProductName FROM bronze.Product) AS src
    ON tgt.ProductAlternateKey = CAST(src.ProductID AS NVARCHAR(25))
    WHEN MATCHED THEN UPDATE SET tgt.EnglishProductName = src.ProductName
    WHEN NOT MATCHED THEN INSERT (ProductAlternateKey, EnglishProductName)
        VALUES (CAST(src.ProductID AS NVARCHAR(25)), src.ProductName);
END
GO
-- Scenario 4: EXEC another proc — internal Command node in body
-- Expected: parse_error set (internal Command), calls not extractable via AST
CREATE PROCEDURE [dbo].[usp_orchestrator]
AS
BEGIN
    EXEC dbo.usp_simple_insert
END
GO
-- Scenario 5: temp table — INSERT into #staging excluded, only silver.dimproduct kept
-- Multiple statements → top-level Command fallback
-- Expected: parse_error set
CREATE PROCEDURE [dbo].[usp_with_temp_table]
AS
BEGIN
    CREATE TABLE #staging (id INT)
    INSERT INTO #staging SELECT ProductID FROM bronze.Product
    INSERT INTO silver.DimProduct (ProductAlternateKey) SELECT CAST(id AS NVARCHAR(25)) FROM #staging
    DROP TABLE #staging
END
GO
-- Scenario 6: dynamic SQL via sp_executesql — EXEC causes internal Command
-- Expected: parse_error set, hidden INSERT not extractable
CREATE PROCEDURE [dbo].[usp_dynamic_sql]
AS
BEGIN
    EXEC sp_executesql N'INSERT INTO silver.DimCustomer (CustomerAlternateKey) SELECT CAST(CustomerID AS NVARCHAR(15)) FROM bronze.Customer'
END
GO
-- Scenario 7: cross-DB 4-part reference in FROM — table.catalog != '' so filtered from reads_from
-- Single INSERT, no internal Commands → parse succeeds
-- Expected: writes_to=[silver.dimproduct], reads_from=[] (cross-DB excluded), calls=[], parse_error=null
CREATE PROCEDURE [dbo].[usp_cross_db]
AS
BEGIN
    INSERT INTO silver.DimProduct (ProductAlternateKey)
    SELECT CAST(ProductID AS NVARCHAR(25)) FROM [OtherDB].[dbo].[ExternalProduct]
END
GO
-- Scenario 8: TRUNCATE + INSERT — multiple statements, TRUNCATE is internal Command
-- Expected: parse_error set (internal Command for TRUNCATE)
CREATE PROCEDURE [dbo].[usp_truncate_insert]
AS
BEGIN
    TRUNCATE TABLE silver.DimCustomer
    INSERT INTO silver.DimCustomer (CustomerAlternateKey)
    SELECT CAST(CustomerID AS NVARCHAR(15)) FROM bronze.Customer
END
GO
