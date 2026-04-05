CREATE TABLE silver.FactSales (
    sale_id BIGINT NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    order_date_key INT NOT NULL,
    ship_date_key INT NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    quantity INT NOT NULL,
    customer_email NVARCHAR(255) NULL,
    load_date DATETIME2 NOT NULL
)
GO

CREATE TABLE silver.DimCustomer (
    customer_sk BIGINT NOT NULL,
    customer_id NVARCHAR(20) NOT NULL,
    first_name NVARCHAR(50) NULL,
    last_name NVARCHAR(50) NULL,
    region NVARCHAR(50) NULL
)
GO

CREATE TABLE silver.DimProduct (
    product_key BIGINT NOT NULL,
    product_name NVARCHAR(100) NOT NULL,
    category NVARCHAR(50) NULL,
    subcategory NVARCHAR(50) NULL,
    brand NVARCHAR(50) NULL,
    load_date DATETIME2 NOT NULL
)
GO
