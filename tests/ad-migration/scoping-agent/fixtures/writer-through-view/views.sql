-- Updatable view — single base table, no aggregation, no DISTINCT
CREATE VIEW [silver].[vw_DimDate]
AS
    SELECT [DateKey], [FullDate], [CalendarYear], [MonthName]
    FROM [silver].[DimDate]
GO
