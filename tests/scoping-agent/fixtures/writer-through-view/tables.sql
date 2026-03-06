CREATE TABLE [silver].[DimDate]
(
    [DateKey] INT NOT NULL,
    [FullDate] DATE NOT NULL,
    [CalendarYear] SMALLINT NOT NULL,
    [MonthName] NVARCHAR(20) NOT NULL,
    CONSTRAINT [PK_silver_DimDate] PRIMARY KEY ([DateKey])
)
GO
