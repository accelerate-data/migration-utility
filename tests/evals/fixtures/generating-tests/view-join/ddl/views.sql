CREATE VIEW silver.vw_CustomerSales
AS
SELECT
    c.CustomerKey,
    c.FirstName,
    c.LastName,
    f.SalesOrderNumber,
    f.OrderDate,
    f.SalesAmount
FROM silver.DimCustomer c
LEFT JOIN silver.FactInternetSales f ON c.CustomerKey = f.CustomerKey;

GO
