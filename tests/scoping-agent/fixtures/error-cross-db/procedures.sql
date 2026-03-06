CREATE PROCEDURE [silver].[usp_load_DimEmployee]
AS
BEGIN
    SET NOCOUNT ON;

    -- Cross-database reference: pulls from HRSystem database
    INSERT INTO [silver].[DimEmployee] ([EmployeeKey], [FirstName], [LastName])
    SELECT
        e.[EmployeeID],
        e.[FirstName],
        e.[LastName]
    FROM [HRSystem].[dbo].[Employees] AS e;
END
GO
