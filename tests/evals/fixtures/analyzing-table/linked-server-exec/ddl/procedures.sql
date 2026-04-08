-- ============================================================
-- SCENARIO: linked-server EXEC — four-part name is out-of-scope
-- ============================================================
CREATE PROCEDURE silver.usp_scope_LinkedServerExec
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [LinkedServer].[WarehouseDb].[silver].[usp_remote_LoadProduct];
END;

GO
