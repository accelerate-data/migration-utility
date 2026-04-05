CREATE PROCEDURE dbo.usp_load_dimcustomer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO [silver].[DimCustomer] AS tgt
    USING [bronze].[CustomerRaw] AS src
        ON tgt.CustomerID = src.CustomerID
    WHEN MATCHED THEN
        UPDATE SET
            FirstName = src.FirstName,
            LastName = src.LastName
    WHEN NOT MATCHED THEN
        INSERT (CustomerID, FirstName, LastName)
        VALUES (src.CustomerID, src.FirstName, src.LastName);
END
