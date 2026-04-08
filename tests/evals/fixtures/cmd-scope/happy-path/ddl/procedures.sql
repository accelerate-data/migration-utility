-- ============================================================
-- SCENARIO: ambiguous_multi_writer — writer B (delta merge)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCustomer_Delta
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimCustomer AS tgt
    USING (
        SELECT
            CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
            p.FirstName, p.MiddleName, p.LastName, p.Title,
            NULL AS Gender, NULL AS MaritalStatus, p.EmailPromotion
        FROM bronze.Customer c
        JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    ) AS src ON tgt.CustomerAlternateKey = src.CustomerAlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.FirstName      = src.FirstName,
        tgt.MiddleName     = src.MiddleName,
        tgt.LastName       = src.LastName,
        tgt.Title          = src.Title,
        tgt.EmailPromotion = src.EmailPromotion
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        CustomerAlternateKey, FirstName, MiddleName, LastName,
        Title, Gender, MaritalStatus, EmailPromotion)
    VALUES (
        src.CustomerAlternateKey, src.FirstName, src.MiddleName, src.LastName,
        src.Title, src.Gender, src.MaritalStatus, src.EmailPromotion);
END;

GO

-- ============================================================
-- SCENARIO: ambiguous_multi_writer — writer A (full reload)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimCustomer_Full
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE silver.DimCustomer;
    INSERT INTO silver.DimCustomer (
        CustomerAlternateKey, FirstName, MiddleName, LastName, Title,
        Gender, MaritalStatus, EmailPromotion, DateFirstPurchase)
    SELECT
        CAST(c.CustomerID AS NVARCHAR(15)) AS CustomerAlternateKey,
        p.FirstName,
        p.MiddleName,
        p.LastName,
        p.Title,
        NULL        AS Gender,
        NULL        AS MaritalStatus,
        p.EmailPromotion,
        CAST(h.MinOrderDate AS DATE) AS DateFirstPurchase
    FROM bronze.Customer c
    JOIN bronze.Person p ON c.PersonID = p.BusinessEntityID
    OUTER APPLY (
        SELECT MIN(OrderDate) AS MinOrderDate
        FROM bronze.SalesOrderHeader sh
        WHERE sh.CustomerID = c.CustomerID
    ) h;
END;

GO

-- ============================================================
-- SCENARIO: resolved (direct writer via MERGE)
-- ============================================================
CREATE   PROCEDURE silver.usp_load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;
    MERGE silver.DimProduct AS tgt
    USING (
        SELECT
            CAST(ProductID AS NVARCHAR(25)) AS ProductAlternateKey,
            ProductName                     AS EnglishProductName,
            StandardCost,
            ListPrice,
            ISNULL(Color, '')               AS Color,
            Size,
            ProductLine,
            Class,
            Style,
            SellStartDate                   AS StartDate,
            SellEndDate                     AS EndDate,
            CASE WHEN DiscontinuedDate IS NOT NULL THEN 'Obsolete'
                 WHEN SellEndDate IS NOT NULL      THEN 'Outdated'
                 ELSE 'Current' END          AS Status
        FROM bronze.Product
    ) AS src ON tgt.ProductAlternateKey = src.ProductAlternateKey
    WHEN MATCHED THEN UPDATE SET
        tgt.EnglishProductName = src.EnglishProductName,
        tgt.StandardCost       = src.StandardCost,
        tgt.ListPrice          = src.ListPrice,
        tgt.Color              = src.Color,
        tgt.Size               = src.Size,
        tgt.ProductLine        = src.ProductLine,
        tgt.Class              = src.Class,
        tgt.Style              = src.Style,
        tgt.Status             = src.Status
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        ProductAlternateKey, EnglishProductName, StandardCost, ListPrice,
        Color, Size, ProductLine, Class, Style, StartDate, EndDate, Status)
    VALUES (
        src.ProductAlternateKey, src.EnglishProductName, src.StandardCost, src.ListPrice,
        src.Color, src.Size, src.ProductLine, src.Class, src.Style, src.StartDate, src.EndDate, src.Status);
END;

GO
