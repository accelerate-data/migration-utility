-- Two procedures both write to silver.DimRegion — ambiguous, agent should flag this
CREATE PROCEDURE [silver].[usp_load_DimRegion_Full]
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE [silver].[DimRegion];

    INSERT INTO [silver].[DimRegion] ([RegionKey], [RegionCode], [RegionName])
    SELECT
        r.[RegionID],
        r.[Code],
        r.[Name]
    FROM [bronze].[Region] AS r;
END
GO

CREATE PROCEDURE [silver].[usp_load_DimRegion_Incremental]
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [silver].[DimRegion] ([RegionKey], [RegionCode], [RegionName])
    SELECT
        r.[RegionID],
        r.[Code],
        r.[Name]
    FROM [bronze].[Region] AS r
    WHERE r.[ModifiedDate] >= CAST(GETDATE() AS DATE);
END
GO
