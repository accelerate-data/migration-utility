CREATE PROCEDURE [silver].[usp_get_geography_report]
AS
BEGIN
    SET NOCOUNT ON;
    -- Read-only: reports on DimGeography but never writes to it
    SELECT [GeographyKey], [Country], [City]
    FROM [silver].[DimGeography]
    ORDER BY [Country];
END
GO
