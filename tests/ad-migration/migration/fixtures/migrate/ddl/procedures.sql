CREATE PROCEDURE [dbo].[usp_load_fact_sales]
AS
BEGIN
    INSERT INTO [silver].[FactSales] (customer_sk, product_sk, amount, load_date)
    SELECT s.CustomerKey, s.ProductKey, s.Amount, GETDATE()
    FROM [bronze].[Sales] s
    WHERE s.Amount > 0
END
GO
CREATE PROCEDURE [dbo].[usp_load_dim_customer]
AS
BEGIN
    MERGE INTO [silver].[DimCustomer] AS tgt
    USING [bronze].[Customer] AS src ON tgt.customer_sk = src.CustomerID
    WHEN MATCHED AND tgt.customer_name <> src.CustomerName THEN
        UPDATE SET tgt.valid_to = GETDATE(), tgt.is_current = 0
    WHEN NOT MATCHED THEN
        INSERT (customer_name, email, valid_from, is_current)
        VALUES (src.CustomerName, src.Email, GETDATE(), 1);
END
GO
CREATE PROCEDURE [dbo].[usp_load_dim_product]
AS
BEGIN
    INSERT INTO [silver].[DimProduct] (product_name, category)
    SELECT ProductName, Category FROM [bronze].[Product]
END
GO
