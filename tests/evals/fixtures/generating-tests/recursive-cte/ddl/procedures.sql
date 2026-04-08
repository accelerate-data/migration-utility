-- ============================================================
-- SCENARIO: Recursive CTE — hierarchical org chart load
-- ============================================================
CREATE PROCEDURE silver.usp_load_RecursiveCteTarget
AS
BEGIN
    SET NOCOUNT ON;
    WITH org AS (
        SELECT
            0 AS OrgLevel,
            NULL AS ManagerId,
            EmployeeID AS EmployeeId,
            CAST(LastName AS NVARCHAR(100)) AS FullPath
        FROM bronze.Employee
        WHERE ManagerID IS NULL
        UNION ALL
        SELECT
            o.OrgLevel + 1,
            e.ManagerID,
            e.EmployeeID,
            CAST(o.FullPath + ' > ' + e.LastName AS NVARCHAR(100))
        FROM bronze.Employee e
        JOIN org o ON e.ManagerID = o.EmployeeId
    )
    INSERT INTO silver.RecursiveCteTarget (OrgLevel, ManagerId, EmployeeId, FullPath)
    SELECT OrgLevel, ManagerId, EmployeeId, FullPath
    FROM org;
END;

GO
