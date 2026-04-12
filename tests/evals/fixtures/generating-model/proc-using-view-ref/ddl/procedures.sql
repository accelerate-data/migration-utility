CREATE OR ALTER PROCEDURE [silver].[usp_load_DimCustomerFromView]
AS
BEGIN
    INSERT INTO [silver].[DimCustomerFromView] (CustomerID, FullName)
    SELECT
        CustomerID,
        FirstName + ' ' + LastName
    FROM [silver].[vw_activecustomers];
END;
