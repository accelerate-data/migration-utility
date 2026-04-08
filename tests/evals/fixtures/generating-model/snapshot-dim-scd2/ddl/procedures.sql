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
