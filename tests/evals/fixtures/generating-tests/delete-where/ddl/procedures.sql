-- ============================================================
-- SCENARIO: DELETE ... WHERE — keep-rows projection
-- ============================================================
CREATE PROCEDURE silver.usp_load_DeleteWhereTarget
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM silver.DeleteWhereTarget
    WHERE IsRetired = 1;
END;

GO
