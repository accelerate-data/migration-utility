-- ============================================================
-- SCENARIO: cross-db exec — writer delegates to another database
-- usp_load_DimCrossDbProfile only EXECs a cross-database
-- procedure, so the profiler cannot inspect the write pattern.
-- Profile status stays partial because the proc body is opaque.
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCrossDbProfile
AS
BEGIN
    SET NOCOUNT ON;
    -- All write logic is in a cross-database procedure; body is opaque
    EXEC [ArchiveDB].[silver].[usp_stage_DimCrossDbProfile];
END;

GO
