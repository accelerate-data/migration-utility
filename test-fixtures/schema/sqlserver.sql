-- ==========================================================
-- Kimball DW Fixture — SQL Server DDL
-- Self-contained, idempotent: drops and recreates all objects
-- ==========================================================

USE KimballFixture;
GO

-- ----------------------------------------------------------
-- Schemas
-- ----------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
    EXEC('CREATE SCHEMA dim');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
    EXEC('CREATE SCHEMA fact');
GO

-- ----------------------------------------------------------
-- Drop existing objects (reverse dependency order)
-- ----------------------------------------------------------

-- Views
IF OBJECT_ID('staging.vw_sales_summary', 'V') IS NOT NULL DROP VIEW staging.vw_sales_summary;
GO
IF OBJECT_ID('staging.vw_stg_sales', 'V') IS NOT NULL DROP VIEW staging.vw_stg_sales;
GO
IF OBJECT_ID('staging.vw_stg_product', 'V') IS NOT NULL DROP VIEW staging.vw_stg_product;
GO
IF OBJECT_ID('staging.vw_stg_customer', 'V') IS NOT NULL DROP VIEW staging.vw_stg_customer;
GO

-- Facts
IF OBJECT_ID('fact.fct_sales_by_channel', 'U') IS NOT NULL DROP TABLE fact.fct_sales_by_channel;
GO
IF OBJECT_ID('fact.fct_sales_summary', 'U') IS NOT NULL DROP TABLE fact.fct_sales_summary;
GO
IF OBJECT_ID('fact.fct_sales', 'U') IS NOT NULL DROP TABLE fact.fct_sales;
GO

-- Dimensions
IF OBJECT_ID('dim.dim_order_status', 'U') IS NOT NULL DROP TABLE dim.dim_order_status;
GO
IF OBJECT_ID('dim.dim_credit_card', 'U') IS NOT NULL DROP TABLE dim.dim_credit_card;
GO
IF OBJECT_ID('dim.dim_address', 'U') IS NOT NULL DROP TABLE dim.dim_address;
GO
IF OBJECT_ID('dim.dim_product_category', 'U') IS NOT NULL DROP TABLE dim.dim_product_category;
GO
IF OBJECT_ID('dim.dim_employee', 'U') IS NOT NULL DROP TABLE dim.dim_employee;
GO
IF OBJECT_ID('dim.dim_date', 'U') IS NOT NULL DROP TABLE dim.dim_date;
GO
IF OBJECT_ID('dim.dim_product', 'U') IS NOT NULL DROP TABLE dim.dim_product;
GO
IF OBJECT_ID('dim.dim_customer', 'U') IS NOT NULL DROP TABLE dim.dim_customer;
GO

-- Staging
IF OBJECT_ID('staging.stg_returns', 'U') IS NOT NULL DROP TABLE staging.stg_returns;
GO
IF OBJECT_ID('staging.stg_employee', 'U') IS NOT NULL DROP TABLE staging.stg_employee;
GO
IF OBJECT_ID('staging.stg_credit_card', 'U') IS NOT NULL DROP TABLE staging.stg_credit_card;
GO
IF OBJECT_ID('staging.stg_address', 'U') IS NOT NULL DROP TABLE staging.stg_address;
GO
IF OBJECT_ID('staging.stg_sales_order_detail', 'U') IS NOT NULL DROP TABLE staging.stg_sales_order_detail;
GO
IF OBJECT_ID('staging.stg_sales_order_header', 'U') IS NOT NULL DROP TABLE staging.stg_sales_order_header;
GO
IF OBJECT_ID('staging.stg_product_category', 'U') IS NOT NULL DROP TABLE staging.stg_product_category;
GO
IF OBJECT_ID('staging.stg_product_subcategory', 'U') IS NOT NULL DROP TABLE staging.stg_product_subcategory;
GO
IF OBJECT_ID('staging.stg_product', 'U') IS NOT NULL DROP TABLE staging.stg_product;
GO
IF OBJECT_ID('staging.stg_person', 'U') IS NOT NULL DROP TABLE staging.stg_person;
GO
IF OBJECT_ID('staging.stg_customer', 'U') IS NOT NULL DROP TABLE staging.stg_customer;
GO

-- ==========================================================
-- Staging tables
-- ==========================================================

CREATE TABLE staging.stg_customer (
    customer_id        INT           NOT NULL,
    person_id          INT           NULL,
    store_id           INT           NULL,
    territory_id       INT           NULL,
    account_number     NVARCHAR(20)  NULL,
    modified_date      DATETIME      NOT NULL,
    CONSTRAINT pk_stg_customer PRIMARY KEY (customer_id)
);
GO

CREATE TABLE staging.stg_person (
    business_entity_id INT           NOT NULL,
    person_type        NCHAR(2)      NOT NULL,
    title              NVARCHAR(8)   NULL,
    first_name         NVARCHAR(50)  NOT NULL,
    middle_name        NVARCHAR(50)  NULL,
    last_name          NVARCHAR(50)  NOT NULL,
    suffix             NVARCHAR(10)  NULL,
    email_promotion    INT           NOT NULL DEFAULT 0,
    modified_date      DATETIME      NOT NULL,
    CONSTRAINT pk_stg_person PRIMARY KEY (business_entity_id)
);
GO

CREATE TABLE staging.stg_product (
    product_id              INT            NOT NULL,
    product_name            NVARCHAR(100)  NOT NULL,
    product_number          NVARCHAR(25)   NOT NULL,
    make_flag               BIT            NOT NULL DEFAULT 1,
    finished_goods_flag     BIT            NOT NULL DEFAULT 1,
    color                   NVARCHAR(15)   NULL,
    safety_stock_level      SMALLINT       NOT NULL,
    reorder_point           SMALLINT       NOT NULL,
    standard_cost           DECIMAL(19,4)  NOT NULL,
    list_price              DECIMAL(19,4)  NOT NULL,
    product_size            NVARCHAR(5)    NULL,
    weight                  DECIMAL(8,2)   NULL,
    days_to_manufacture     INT            NOT NULL,
    product_line            NCHAR(2)       NULL,
    class                   NCHAR(2)       NULL,
    style                   NCHAR(2)       NULL,
    product_subcategory_id  INT            NULL,
    product_model_id        INT            NULL,
    sell_start_date         DATETIME       NOT NULL,
    sell_end_date           DATETIME       NULL,
    discontinued_date       DATETIME       NULL,
    modified_date           DATETIME       NOT NULL,
    CONSTRAINT pk_stg_product PRIMARY KEY (product_id)
);
GO

CREATE TABLE staging.stg_product_subcategory (
    product_subcategory_id  INT            NOT NULL,
    product_category_id     INT            NOT NULL,
    subcategory_name        NVARCHAR(50)   NOT NULL,
    modified_date           DATETIME       NOT NULL,
    CONSTRAINT pk_stg_product_subcategory PRIMARY KEY (product_subcategory_id)
);
GO

CREATE TABLE staging.stg_product_category (
    product_category_id  INT            NOT NULL,
    category_name        NVARCHAR(50)   NOT NULL,
    modified_date        DATETIME       NOT NULL,
    CONSTRAINT pk_stg_product_category PRIMARY KEY (product_category_id)
);
GO

CREATE TABLE staging.stg_sales_order_header (
    sales_order_id          INT            NOT NULL,
    revision_number         SMALLINT       NOT NULL DEFAULT 0,
    order_date              DATETIME       NOT NULL,
    due_date                DATETIME       NOT NULL,
    ship_date               DATETIME       NULL,
    status                  SMALLINT       NOT NULL DEFAULT 1,
    online_order_flag       BIT            NOT NULL DEFAULT 1,
    sales_order_number      NVARCHAR(25)   NULL,
    customer_id             INT            NOT NULL,
    sales_person_id         INT            NULL,
    territory_id            INT            NULL,
    bill_to_address_id      INT            NOT NULL,
    ship_to_address_id      INT            NOT NULL,
    ship_method_id          INT            NOT NULL,
    credit_card_id          INT            NULL,
    sub_total               DECIMAL(19,4)  NOT NULL DEFAULT 0,
    tax_amt                 DECIMAL(19,4)  NOT NULL DEFAULT 0,
    freight                 DECIMAL(19,4)  NOT NULL DEFAULT 0,
    total_due               DECIMAL(19,4)  NOT NULL,
    modified_date           DATETIME       NOT NULL,
    CONSTRAINT pk_stg_sales_order_header PRIMARY KEY (sales_order_id)
);
GO

CREATE TABLE staging.stg_sales_order_detail (
    sales_order_id          INT            NOT NULL,
    sales_order_detail_id   INT            NOT NULL,
    carrier_tracking_number NVARCHAR(25)   NULL,
    order_qty               SMALLINT       NOT NULL,
    product_id              INT            NOT NULL,
    special_offer_id        INT            NOT NULL,
    unit_price              DECIMAL(19,4)  NOT NULL,
    unit_price_discount     DECIMAL(19,4)  NOT NULL DEFAULT 0,
    line_total              DECIMAL(38,6)  NOT NULL,
    modified_date           DATETIME       NOT NULL,
    CONSTRAINT pk_stg_sales_order_detail PRIMARY KEY (sales_order_id, sales_order_detail_id)
);
GO

CREATE TABLE staging.stg_address (
    address_id        INT            NOT NULL,
    address_line_1    NVARCHAR(60)   NOT NULL,
    address_line_2    NVARCHAR(60)   NULL,
    city              NVARCHAR(30)   NOT NULL,
    state_province_id INT            NOT NULL,
    postal_code       NVARCHAR(15)   NOT NULL,
    modified_date     DATETIME       NOT NULL,
    CONSTRAINT pk_stg_address PRIMARY KEY (address_id)
);
GO

CREATE TABLE staging.stg_credit_card (
    credit_card_id  INT            NOT NULL,
    card_type       NVARCHAR(50)   NOT NULL,
    card_number     NVARCHAR(25)   NOT NULL,
    exp_month       SMALLINT       NOT NULL,
    exp_year        SMALLINT       NOT NULL,
    modified_date   DATETIME       NOT NULL,
    CONSTRAINT pk_stg_credit_card PRIMARY KEY (credit_card_id)
);
GO

CREATE TABLE staging.stg_employee (
    business_entity_id  INT            NOT NULL,
    national_id_number  NVARCHAR(15)   NOT NULL,
    login_id            NVARCHAR(256)  NOT NULL,
    job_title           NVARCHAR(50)   NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              NCHAR(1)       NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       BIT            NOT NULL DEFAULT 1,
    vacation_hours      SMALLINT       NOT NULL DEFAULT 0,
    sick_leave_hours    SMALLINT       NOT NULL DEFAULT 0,
    current_flag        BIT            NOT NULL DEFAULT 1,
    first_name          NVARCHAR(50)   NOT NULL,
    last_name           NVARCHAR(50)   NOT NULL,
    modified_date       DATETIME       NOT NULL,
    CONSTRAINT pk_stg_employee PRIMARY KEY (business_entity_id)
);
GO

CREATE TABLE staging.stg_returns (
    return_id              INT            NOT NULL,
    sales_order_id         INT            NOT NULL,
    sales_order_detail_id  INT            NOT NULL,
    return_date            DATETIME       NOT NULL,
    return_qty             SMALLINT       NOT NULL,
    return_reason          NVARCHAR(100)  NULL,
    CONSTRAINT pk_stg_returns PRIMARY KEY (return_id)
);
GO

-- ==========================================================
-- Dimension tables
-- ==========================================================

CREATE TABLE dim.dim_customer (
    customer_key     INT IDENTITY(1,1) NOT NULL,
    customer_id      INT            NOT NULL,
    person_id        INT            NULL,
    store_id         INT            NULL,
    full_name        NVARCHAR(150)  NULL,
    store_name       NVARCHAR(100)  NULL,
    territory_id     INT            NULL,
    valid_from       DATETIME       NOT NULL,
    valid_to         DATETIME       NULL,
    is_current       BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);
GO

CREATE TABLE dim.dim_product (
    product_key             INT IDENTITY(1,1) NOT NULL,
    product_id              INT            NOT NULL,
    product_name            NVARCHAR(100)  NOT NULL,
    product_number          NVARCHAR(25)   NOT NULL,
    color                   NVARCHAR(15)   NULL,
    class                   NCHAR(2)       NULL,
    product_line            NCHAR(2)       NULL,
    standard_cost           DECIMAL(19,4)  NOT NULL,
    list_price              DECIMAL(19,4)  NOT NULL,
    product_subcategory     NVARCHAR(50)   NULL,
    product_category        NVARCHAR(50)   NULL,
    sell_start_date         DATETIME       NOT NULL,
    sell_end_date           DATETIME       NULL,
    valid_from              DATETIME       NOT NULL,
    valid_to                DATETIME       NULL,
    is_current              BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
);
GO

CREATE TABLE dim.dim_date (
    date_key               INT          NOT NULL,
    date_day               DATE         NOT NULL,
    day_of_week            SMALLINT     NOT NULL,
    day_of_week_name       NVARCHAR(10) NOT NULL,
    day_of_month           SMALLINT     NOT NULL,
    day_of_year            SMALLINT     NOT NULL,
    week_of_year           SMALLINT     NOT NULL,
    month_number           SMALLINT     NOT NULL,
    month_name             NVARCHAR(10) NOT NULL,
    quarter_number         SMALLINT     NOT NULL,
    year_number            INT          NOT NULL,
    is_weekend             BIT          NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);
GO

CREATE TABLE dim.dim_employee (
    employee_key        INT IDENTITY(1,1) NOT NULL,
    employee_id         INT            NOT NULL,
    national_id_number  NVARCHAR(15)   NOT NULL,
    first_name          NVARCHAR(50)   NOT NULL,
    last_name           NVARCHAR(50)   NOT NULL,
    job_title           NVARCHAR(50)   NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              NCHAR(1)       NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       BIT            NOT NULL DEFAULT 1,
    current_flag        BIT            NOT NULL DEFAULT 1,
    valid_from          DATETIME       NOT NULL,
    valid_to            DATETIME       NULL,
    is_current          BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key)
);
GO

CREATE TABLE dim.dim_product_category (
    product_category_key  INT IDENTITY(1,1) NOT NULL,
    product_category_id   INT            NOT NULL,
    category_name         NVARCHAR(50)   NOT NULL,
    valid_from            DATETIME       NOT NULL,
    valid_to              DATETIME       NULL,
    is_current            BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_product_category PRIMARY KEY (product_category_key)
);
GO

CREATE TABLE dim.dim_address (
    address_key       INT IDENTITY(1,1) NOT NULL,
    address_id        INT            NOT NULL,
    address_line_1    NVARCHAR(60)   NOT NULL,
    city              NVARCHAR(30)   NOT NULL,
    state_province_id INT            NOT NULL,
    postal_code       NVARCHAR(15)   NOT NULL,
    valid_from        DATETIME       NOT NULL,
    valid_to          DATETIME       NULL,
    is_current        BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_address PRIMARY KEY (address_key)
);
GO

CREATE TABLE dim.dim_credit_card (
    credit_card_key  INT IDENTITY(1,1) NOT NULL,
    credit_card_id   INT            NOT NULL,
    card_type        NVARCHAR(50)   NOT NULL,
    exp_month        SMALLINT       NOT NULL,
    exp_year         SMALLINT       NOT NULL,
    valid_from       DATETIME       NOT NULL,
    valid_to         DATETIME       NULL,
    is_current       BIT            NOT NULL DEFAULT 1,
    CONSTRAINT pk_dim_credit_card PRIMARY KEY (credit_card_key)
);
GO

CREATE TABLE dim.dim_order_status (
    order_status_key   INT IDENTITY(1,1) NOT NULL,
    order_status       SMALLINT       NOT NULL,
    order_status_name  NVARCHAR(20)   NOT NULL,
    CONSTRAINT pk_dim_order_status PRIMARY KEY (order_status_key)
);
GO

-- ==========================================================
-- Fact tables
-- ==========================================================

CREATE TABLE fact.fct_sales (
    sales_key              INT IDENTITY(1,1) NOT NULL,
    sales_order_id         INT            NOT NULL,
    sales_order_detail_id  INT            NOT NULL,
    customer_key           INT            NOT NULL,
    product_key            INT            NOT NULL,
    date_key               INT            NOT NULL,
    address_key            INT            NULL,
    credit_card_key        INT            NULL,
    order_status_key       INT            NOT NULL,
    order_qty              SMALLINT       NOT NULL,
    unit_price             DECIMAL(19,4)  NOT NULL,
    unit_price_discount    DECIMAL(19,4)  NOT NULL DEFAULT 0,
    line_total             DECIMAL(38,6)  NOT NULL,
    CONSTRAINT pk_fct_sales PRIMARY KEY (sales_key)
);
GO

CREATE TABLE fact.fct_sales_summary (
    sales_summary_key  INT IDENTITY(1,1) NOT NULL,
    date_key           INT            NOT NULL,
    product_key        INT            NOT NULL,
    total_qty          INT            NOT NULL,
    total_revenue      DECIMAL(38,6)  NOT NULL,
    order_count        INT            NOT NULL,
    CONSTRAINT pk_fct_sales_summary PRIMARY KEY (sales_summary_key)
);
GO

CREATE TABLE fact.fct_sales_by_channel (
    sales_channel_key  INT IDENTITY(1,1) NOT NULL,
    date_key           INT            NOT NULL,
    channel            NVARCHAR(10)   NOT NULL,
    total_qty          INT            NOT NULL,
    total_revenue      DECIMAL(38,6)  NOT NULL,
    order_count        INT            NOT NULL,
    CONSTRAINT pk_fct_sales_by_channel PRIMARY KEY (sales_channel_key)
);
GO

-- ==========================================================
-- Views
-- ==========================================================

CREATE VIEW staging.vw_stg_customer AS
SELECT
    c.customer_id,
    c.person_id,
    c.store_id,
    c.territory_id,
    CONCAT(p.first_name, ' ', COALESCE(p.middle_name + ' ', ''), p.last_name) AS full_name,
    c.modified_date
FROM staging.stg_customer c
LEFT JOIN staging.stg_person p ON c.person_id = p.business_entity_id
WHERE c.customer_id IS NOT NULL;
GO

CREATE VIEW staging.vw_stg_product AS
SELECT
    product_id,
    product_name,
    product_number,
    color,
    class,
    product_line,
    CAST(standard_cost AS DECIMAL(19,4)) AS standard_cost,
    CAST(list_price AS DECIMAL(19,4))    AS list_price,
    product_subcategory_id,
    CASE WHEN sell_end_date IS NULL AND discontinued_date IS NULL THEN 1 ELSE 0 END AS is_active,
    sell_start_date,
    sell_end_date,
    modified_date
FROM staging.stg_product;
GO

CREATE VIEW staging.vw_stg_sales AS
SELECT
    d.sales_order_id,
    d.sales_order_detail_id,
    d.order_qty,
    d.product_id,
    d.unit_price,
    d.unit_price_discount,
    d.line_total,
    CASE WHEN r.return_id IS NOT NULL THEN 1 ELSE 0 END AS is_returned,
    d.modified_date
FROM staging.stg_sales_order_detail d
LEFT JOIN staging.stg_returns r
    ON d.sales_order_id = r.sales_order_id
    AND d.sales_order_detail_id = r.sales_order_detail_id;
GO

CREATE VIEW staging.vw_sales_summary AS
SELECT
    CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
    d.product_id,
    SUM(d.order_qty)   AS total_qty,
    SUM(d.line_total)  AS total_revenue,
    COUNT(DISTINCT h.sales_order_id) AS order_count
FROM staging.stg_sales_order_header h
INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
GROUP BY CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT), d.product_id;
GO
