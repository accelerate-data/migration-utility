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
