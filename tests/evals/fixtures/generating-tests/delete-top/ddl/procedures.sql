-- ============================================================
-- SCENARIO: DELETE TOP — delete oldest records
-- ============================================================
CREATE PROCEDURE silver.usp_load_DeleteTopTarget
AS
BEGIN
    SET NOCOUNT ON;
    DELETE TOP (100) FROM silver.DeleteTopTarget
    WHERE LoadedDate < DATEADD(YEAR, -1, GETDATE());
END;

GO
