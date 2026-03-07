CREATE PROCEDURE [silver].[usp_load_DimDate]
AS
BEGIN
    SET NOCOUNT ON;

    -- Writes via updatable view vw_DimDate, which maps directly to silver.DimDate
    TRUNCATE TABLE [silver].[DimDate];

    INSERT INTO [silver].[vw_DimDate] ([DateKey], [FullDate], [CalendarYear], [MonthName])
    SELECT
        d.[DateKey],
        d.[FullDate],
        d.[CalendarYear],
        d.[MonthName]
    FROM [bronze].[DimDate] AS d;
END
GO
