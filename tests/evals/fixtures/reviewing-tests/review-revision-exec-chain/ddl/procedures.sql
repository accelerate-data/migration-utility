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
