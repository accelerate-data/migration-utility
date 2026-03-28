CREATE FUNCTION [dbo].[fn_GetRegion](@GeoId INT)
RETURNS NVARCHAR(50)
AS BEGIN
    RETURN (SELECT Region FROM bronze.Geography WHERE GeographyId = @GeoId)
END
GO
