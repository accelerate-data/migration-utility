-- Top-level orchestrator calls a sub-proc; the sub-proc does the actual INSERT
-- Agent must traverse the call graph to find the real writer
CREATE PROCEDURE [silver].[usp_load_DimTerritory_Inner]
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [silver].[DimTerritory] ([TerritoryKey], [TerritoryCode], [TerritoryName], [RegionGroup])
    SELECT
        t.[TerritoryID],
        t.[Code],
        t.[Name],
        t.[RegionGroup]
    FROM [bronze].[Territory] AS t;
END
GO

CREATE PROCEDURE [silver].[usp_load_DimTerritory]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE [silver].[DimTerritory];

    -- Delegates actual load to inner proc
    EXEC [silver].[usp_load_DimTerritory_Inner];
END
GO
