-- ============================================================
-- SCENARIO: PII-rich dimension — contact info with sensitive data
-- ============================================================
CREATE PROCEDURE silver.usp_load_DimContact
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimContact;
    INSERT INTO silver.DimContact (
        ContactAlternateKey, FirstName, LastName,
        EmailAddress, PhoneNumber, SocialSecurityNumber,
        BirthDate, StreetAddress, City, PostalCode)
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)),
        p.FirstName,
        p.LastName,
        LOWER(p.FirstName + '.' + p.LastName + '@example.com'),
        '555-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4) + '-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4),
        RIGHT('000' + CAST(c.CustomerID % 1000 AS VARCHAR), 3) + '-' + RIGHT('00' + CAST((c.CustomerID / 1000) % 100 AS VARCHAR), 2) + '-' + RIGHT('0000' + CAST(c.CustomerID AS VARCHAR), 4),
        DATEADD(DAY, -(c.CustomerID % 10000), GETDATE()),
        CAST(c.CustomerID AS NVARCHAR) + ' Main Street',
        'Anytown',
        RIGHT('00000' + CAST(c.CustomerID % 100000 AS VARCHAR), 5)
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID;
END;

GO
