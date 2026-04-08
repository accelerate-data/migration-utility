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
