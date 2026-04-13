CREATE PROCEDURE dbo.usp_load_fact_sales
    @batch_date DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.FactSales (
        sale_id, customer_key, product_key,
        order_date_key, ship_date_key,
        amount, quantity, customer_email, load_date
    )
    SELECT
        s.sale_id,
        c.customer_key,
        p.product_key,
        d1.date_key AS order_date_key,
        d2.date_key AS ship_date_key,
        s.amount,
        s.quantity,
        s.customer_email,
        GETDATE()
    FROM bronze.SalesRaw s
    JOIN silver.DimCustomer c ON s.customer_id = c.customer_id
    JOIN silver.DimProduct p ON s.product_id = p.product_id
    JOIN silver.DimDate d1 ON s.order_date = d1.full_date
    JOIN silver.DimDate d2 ON s.ship_date = d2.full_date
    WHERE s.load_date > @batch_date;
END
GO

CREATE PROCEDURE dbo.usp_merge_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE silver.DimCustomer AS tgt
    USING bronze.CustomerStaging AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.first_name = src.first_name,
            tgt.last_name = src.last_name,
            tgt.region = src.region
    WHEN NOT MATCHED THEN
        INSERT (customer_sk, customer_id, first_name, last_name, region)
        VALUES (NEXT VALUE FOR dbo.seq_customer_sk, src.customer_id, src.first_name, src.last_name, src.region);
END
GO

CREATE PROCEDURE dbo.usp_helper_log
    @message NVARCHAR(500)
AS
BEGIN
    INSERT INTO dbo.AuditLog (message, logged_at)
    VALUES (@message, GETDATE());
END
GO

CREATE PROCEDURE dbo.usp_truncate_insert_dim_product
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE silver.DimProduct;

    INSERT INTO silver.DimProduct (
        product_key, product_name, category, subcategory, brand, load_date
    )
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        GETDATE()
    FROM bronze.ProductStaging p;
END
GO
