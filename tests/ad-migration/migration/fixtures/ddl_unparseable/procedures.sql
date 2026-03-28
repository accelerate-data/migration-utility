-- Scenario 2: nested IF/ELSE with BEGIN/END — sqlglot falls back to top-level Command
-- Expected: DdlParseError raised
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
-- Expected: DdlParseError raised
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
-- Expected: parse succeeds at top level, extract_refs raises DdlParseError
CREATE PROCEDURE [dbo].[usp_orchestrator]
AS
BEGIN
    EXEC dbo.usp_simple_insert
END
GO
-- Scenario 5: temp table — multiple statements → top-level Command fallback
-- Expected: DdlParseError raised
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
-- Expected: parse succeeds at top level, extract_refs raises DdlParseError
CREATE PROCEDURE [dbo].[usp_dynamic_sql]
AS
BEGIN
    EXEC sp_executesql N'INSERT INTO silver.DimCustomer (CustomerAlternateKey) SELECT CAST(CustomerID AS NVARCHAR(15)) FROM bronze.Customer'
END
GO
-- Scenario 8: TRUNCATE + INSERT — multiple statements, TRUNCATE is internal Command
-- Expected: parse succeeds at top level, extract_refs raises DdlParseError
CREATE PROCEDURE [dbo].[usp_truncate_insert]
AS
BEGIN
    TRUNCATE TABLE silver.DimCustomer
    INSERT INTO silver.DimCustomer (CustomerAlternateKey)
    SELECT CAST(CustomerID AS NVARCHAR(15)) FROM bronze.Customer
END
GO
