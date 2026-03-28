CREATE VIEW silver.vw_CustomerSales
AS
SELECT c.FirstName, SUM(f.Amount) AS TotalSales
FROM silver.DimCustomer c
JOIN silver.FactSales f ON c.CustomerKey = f.CustomerKey
GROUP BY c.FirstName;
GO
