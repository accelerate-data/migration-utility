-- ==========================================================
-- Kimball DW Fixture — PostgreSQL DDL
-- Self-contained, idempotent: drops and recreates all objects
-- Run as: superuser against kimball_fixture database
-- ==========================================================

-- ----------------------------------------------------------
-- Schemas
-- ----------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;

-- ----------------------------------------------------------
-- Drop existing objects (reverse dependency order)
-- ----------------------------------------------------------

-- Views
DROP VIEW IF EXISTS staging.vw_sales_summary CASCADE;
DROP VIEW IF EXISTS staging.vw_stg_sales CASCADE;
DROP VIEW IF EXISTS staging.vw_stg_product CASCADE;
DROP VIEW IF EXISTS staging.vw_stg_customer CASCADE;

-- Facts
DROP TABLE IF EXISTS fact.fct_sales_by_channel CASCADE;
DROP TABLE IF EXISTS fact.fct_sales_summary CASCADE;
DROP TABLE IF EXISTS fact.fct_sales CASCADE;

-- Dimensions
DROP TABLE IF EXISTS dim.dim_order_status CASCADE;
DROP TABLE IF EXISTS dim.dim_credit_card CASCADE;
DROP TABLE IF EXISTS dim.dim_address CASCADE;
DROP TABLE IF EXISTS dim.dim_product_category CASCADE;
DROP TABLE IF EXISTS dim.dim_employee CASCADE;
DROP TABLE IF EXISTS dim.dim_date CASCADE;
DROP TABLE IF EXISTS dim.dim_product CASCADE;
DROP TABLE IF EXISTS dim.dim_customer CASCADE;

-- Staging
DROP TABLE IF EXISTS staging.stg_returns CASCADE;
DROP TABLE IF EXISTS staging.stg_employee CASCADE;
DROP TABLE IF EXISTS staging.stg_credit_card CASCADE;
DROP TABLE IF EXISTS staging.stg_address CASCADE;
DROP TABLE IF EXISTS staging.stg_sales_order_detail CASCADE;
DROP TABLE IF EXISTS staging.stg_sales_order_header CASCADE;
DROP TABLE IF EXISTS staging.stg_product_category CASCADE;
DROP TABLE IF EXISTS staging.stg_product_subcategory CASCADE;
DROP TABLE IF EXISTS staging.stg_product CASCADE;
DROP TABLE IF EXISTS staging.stg_person CASCADE;
DROP TABLE IF EXISTS staging.stg_customer CASCADE;

-- ==========================================================
-- Staging tables
-- ==========================================================

CREATE TABLE staging.stg_customer (
    customer_id        INTEGER        NOT NULL,
    person_id          INTEGER        NULL,
    store_id           INTEGER        NULL,
    territory_id       INTEGER        NULL,
    account_number     VARCHAR(20)    NULL,
    modified_date      TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_customer PRIMARY KEY (customer_id)
);

CREATE TABLE staging.stg_person (
    business_entity_id INTEGER        NOT NULL,
    person_type        CHAR(2)        NOT NULL,
    title              VARCHAR(8)     NULL,
    first_name         VARCHAR(50)    NOT NULL,
    middle_name        VARCHAR(50)    NULL,
    last_name          VARCHAR(50)    NOT NULL,
    suffix             VARCHAR(10)    NULL,
    email_promotion    INTEGER        NOT NULL DEFAULT 0,
    modified_date      TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_person PRIMARY KEY (business_entity_id)
);

CREATE TABLE staging.stg_product (
    product_id              INTEGER        NOT NULL,
    product_name            VARCHAR(100)   NOT NULL,
    product_number          VARCHAR(25)    NOT NULL,
    make_flag               BOOLEAN        NOT NULL DEFAULT TRUE,
    finished_goods_flag     BOOLEAN        NOT NULL DEFAULT TRUE,
    color                   VARCHAR(15)    NULL,
    safety_stock_level      SMALLINT       NOT NULL,
    reorder_point           SMALLINT       NOT NULL,
    standard_cost           NUMERIC(19,4)  NOT NULL,
    list_price              NUMERIC(19,4)  NOT NULL,
    product_size            VARCHAR(5)     NULL,
    weight                  NUMERIC(8,2)   NULL,
    days_to_manufacture     INTEGER        NOT NULL,
    product_line            CHAR(2)        NULL,
    class                   CHAR(2)        NULL,
    style                   CHAR(2)        NULL,
    product_subcategory_id  INTEGER        NULL,
    product_model_id        INTEGER        NULL,
    sell_start_date         TIMESTAMP      NOT NULL,
    sell_end_date           TIMESTAMP      NULL,
    discontinued_date       TIMESTAMP      NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product PRIMARY KEY (product_id)
);

CREATE TABLE staging.stg_product_subcategory (
    product_subcategory_id  INTEGER        NOT NULL,
    product_category_id     INTEGER        NOT NULL,
    subcategory_name        VARCHAR(50)    NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product_subcategory PRIMARY KEY (product_subcategory_id)
);

CREATE TABLE staging.stg_product_category (
    product_category_id  INTEGER        NOT NULL,
    category_name        VARCHAR(50)    NOT NULL,
    modified_date        TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product_category PRIMARY KEY (product_category_id)
);

CREATE TABLE staging.stg_sales_order_header (
    sales_order_id          INTEGER        NOT NULL,
    revision_number         SMALLINT       NOT NULL DEFAULT 0,
    order_date              TIMESTAMP      NOT NULL,
    due_date                TIMESTAMP      NOT NULL,
    ship_date               TIMESTAMP      NULL,
    status                  SMALLINT       NOT NULL DEFAULT 1,
    online_order_flag       BOOLEAN        NOT NULL DEFAULT TRUE,
    sales_order_number      VARCHAR(25)    NULL,
    customer_id             INTEGER        NOT NULL,
    sales_person_id         INTEGER        NULL,
    territory_id            INTEGER        NULL,
    bill_to_address_id      INTEGER        NOT NULL,
    ship_to_address_id      INTEGER        NOT NULL,
    ship_method_id          INTEGER        NOT NULL,
    credit_card_id          INTEGER        NULL,
    sub_total               NUMERIC(19,4)  NOT NULL DEFAULT 0,
    tax_amt                 NUMERIC(19,4)  NOT NULL DEFAULT 0,
    freight                 NUMERIC(19,4)  NOT NULL DEFAULT 0,
    total_due               NUMERIC(19,4)  NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_sales_order_header PRIMARY KEY (sales_order_id)
);

CREATE TABLE staging.stg_sales_order_detail (
    sales_order_id          INTEGER        NOT NULL,
    sales_order_detail_id   INTEGER        NOT NULL,
    carrier_tracking_number VARCHAR(25)    NULL,
    order_qty               SMALLINT       NOT NULL,
    product_id              INTEGER        NOT NULL,
    special_offer_id        INTEGER        NOT NULL,
    unit_price              NUMERIC(19,4)  NOT NULL,
    unit_price_discount     NUMERIC(19,4)  NOT NULL DEFAULT 0,
    line_total              NUMERIC(38,6)  NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_sales_order_detail PRIMARY KEY (sales_order_id, sales_order_detail_id)
);

CREATE TABLE staging.stg_address (
    address_id        INTEGER        NOT NULL,
    address_line_1    VARCHAR(60)    NOT NULL,
    address_line_2    VARCHAR(60)    NULL,
    city              VARCHAR(30)    NOT NULL,
    state_province_id INTEGER        NOT NULL,
    postal_code       VARCHAR(15)    NOT NULL,
    modified_date     TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_address PRIMARY KEY (address_id)
);

CREATE TABLE staging.stg_credit_card (
    credit_card_id  INTEGER        NOT NULL,
    card_type       VARCHAR(50)    NOT NULL,
    card_number     VARCHAR(25)    NOT NULL,
    exp_month       SMALLINT       NOT NULL,
    exp_year        SMALLINT       NOT NULL,
    modified_date   TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_credit_card PRIMARY KEY (credit_card_id)
);

CREATE TABLE staging.stg_employee (
    business_entity_id  INTEGER        NOT NULL,
    national_id_number  VARCHAR(15)    NOT NULL,
    login_id            VARCHAR(256)   NOT NULL,
    job_title           VARCHAR(50)    NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              CHAR(1)        NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       BOOLEAN        NOT NULL DEFAULT TRUE,
    vacation_hours      SMALLINT       NOT NULL DEFAULT 0,
    sick_leave_hours    SMALLINT       NOT NULL DEFAULT 0,
    current_flag        BOOLEAN        NOT NULL DEFAULT TRUE,
    first_name          VARCHAR(50)    NOT NULL,
    last_name           VARCHAR(50)    NOT NULL,
    modified_date       TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_employee PRIMARY KEY (business_entity_id)
);

CREATE TABLE staging.stg_returns (
    return_id              INTEGER        NOT NULL,
    sales_order_id         INTEGER        NOT NULL,
    sales_order_detail_id  INTEGER        NOT NULL,
    return_date            TIMESTAMP      NOT NULL,
    return_qty             SMALLINT       NOT NULL,
    return_reason          VARCHAR(100)   NULL,
    CONSTRAINT pk_stg_returns PRIMARY KEY (return_id)
);

-- ==========================================================
-- Dimension tables
-- ==========================================================

CREATE TABLE dim.dim_customer (
    customer_key     INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id      INTEGER        NOT NULL,
    person_id        INTEGER        NULL,
    store_id         INTEGER        NULL,
    full_name        VARCHAR(150)   NULL,
    store_name       VARCHAR(100)   NULL,
    territory_id     INTEGER        NULL,
    valid_from       TIMESTAMP      NOT NULL,
    valid_to         TIMESTAMP      NULL,
    is_current       BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);

CREATE TABLE dim.dim_product (
    product_key             INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_id              INTEGER        NOT NULL,
    product_name            VARCHAR(100)   NOT NULL,
    product_number          VARCHAR(25)    NOT NULL,
    color                   VARCHAR(15)    NULL,
    class                   CHAR(2)        NULL,
    product_line            CHAR(2)        NULL,
    standard_cost           NUMERIC(19,4)  NOT NULL,
    list_price              NUMERIC(19,4)  NOT NULL,
    product_subcategory     VARCHAR(50)    NULL,
    product_category        VARCHAR(50)    NULL,
    sell_start_date         TIMESTAMP      NOT NULL,
    sell_end_date           TIMESTAMP      NULL,
    valid_from              TIMESTAMP      NOT NULL,
    valid_to                TIMESTAMP      NULL,
    is_current              BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
);

CREATE TABLE dim.dim_date (
    date_key               INTEGER        NOT NULL,
    date_day               DATE           NOT NULL,
    day_of_week            SMALLINT       NOT NULL,
    day_of_week_name       VARCHAR(10)    NOT NULL,
    day_of_month           SMALLINT       NOT NULL,
    day_of_year            SMALLINT       NOT NULL,
    week_of_year           SMALLINT       NOT NULL,
    month_number           SMALLINT       NOT NULL,
    month_name             VARCHAR(10)    NOT NULL,
    quarter_number         SMALLINT       NOT NULL,
    year_number            INTEGER        NOT NULL,
    is_weekend             BOOLEAN        NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE TABLE dim.dim_employee (
    employee_key        INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    employee_id         INTEGER        NOT NULL,
    national_id_number  VARCHAR(15)    NOT NULL,
    first_name          VARCHAR(50)    NOT NULL,
    last_name           VARCHAR(50)    NOT NULL,
    job_title           VARCHAR(50)    NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              CHAR(1)        NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       BOOLEAN        NOT NULL DEFAULT TRUE,
    current_flag        BOOLEAN        NOT NULL DEFAULT TRUE,
    valid_from          TIMESTAMP      NOT NULL,
    valid_to            TIMESTAMP      NULL,
    is_current          BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key)
);

CREATE TABLE dim.dim_product_category (
    product_category_key  INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_category_id   INTEGER        NOT NULL,
    category_name         VARCHAR(50)    NOT NULL,
    valid_from            TIMESTAMP      NOT NULL,
    valid_to              TIMESTAMP      NULL,
    is_current            BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_product_category PRIMARY KEY (product_category_key)
);

CREATE TABLE dim.dim_address (
    address_key       INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    address_id        INTEGER        NOT NULL,
    address_line_1    VARCHAR(60)    NOT NULL,
    city              VARCHAR(30)    NOT NULL,
    state_province_id INTEGER        NOT NULL,
    postal_code       VARCHAR(15)    NOT NULL,
    valid_from        TIMESTAMP      NOT NULL,
    valid_to          TIMESTAMP      NULL,
    is_current        BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_address PRIMARY KEY (address_key)
);

CREATE TABLE dim.dim_credit_card (
    credit_card_key  INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    credit_card_id   INTEGER        NOT NULL,
    card_type        VARCHAR(50)    NOT NULL,
    exp_month        SMALLINT       NOT NULL,
    exp_year         SMALLINT       NOT NULL,
    valid_from       TIMESTAMP      NOT NULL,
    valid_to         TIMESTAMP      NULL,
    is_current       BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_credit_card PRIMARY KEY (credit_card_key)
);

CREATE TABLE dim.dim_order_status (
    order_status_key   INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    order_status       SMALLINT       NOT NULL,
    order_status_name  VARCHAR(20)    NOT NULL,
    CONSTRAINT pk_dim_order_status PRIMARY KEY (order_status_key)
);

-- ==========================================================
-- Fact tables
-- ==========================================================

CREATE TABLE fact.fct_sales (
    sales_key              INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    sales_order_id         INTEGER        NOT NULL,
    sales_order_detail_id  INTEGER        NOT NULL,
    customer_key           INTEGER        NOT NULL,
    product_key            INTEGER        NOT NULL,
    date_key               INTEGER        NOT NULL,
    address_key            INTEGER        NULL,
    credit_card_key        INTEGER        NULL,
    order_status_key       INTEGER        NOT NULL,
    order_qty              SMALLINT       NOT NULL,
    unit_price             NUMERIC(19,4)  NOT NULL,
    unit_price_discount    NUMERIC(19,4)  NOT NULL DEFAULT 0,
    line_total             NUMERIC(38,6)  NOT NULL,
    CONSTRAINT pk_fct_sales PRIMARY KEY (sales_key)
);

CREATE TABLE fact.fct_sales_summary (
    sales_summary_key  INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    date_key           INTEGER        NOT NULL,
    product_key        INTEGER        NOT NULL,
    total_qty          INTEGER        NOT NULL,
    total_revenue      NUMERIC(38,6)  NOT NULL,
    order_count        INTEGER        NOT NULL,
    CONSTRAINT pk_fct_sales_summary PRIMARY KEY (sales_summary_key)
);

CREATE TABLE fact.fct_sales_by_channel (
    sales_channel_key  INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    date_key           INTEGER        NOT NULL,
    channel            VARCHAR(10)    NOT NULL,
    total_qty          INTEGER        NOT NULL,
    total_revenue      NUMERIC(38,6)  NOT NULL,
    order_count        INTEGER        NOT NULL,
    CONSTRAINT pk_fct_sales_by_channel PRIMARY KEY (sales_channel_key)
);

-- ==========================================================
-- Views
-- ==========================================================

CREATE VIEW staging.vw_stg_customer AS
SELECT
    c.customer_id,
    c.person_id,
    c.store_id,
    c.territory_id,
    CONCAT(p.first_name, ' ', COALESCE(p.middle_name || ' ', ''), p.last_name) AS full_name,
    c.modified_date
FROM staging.stg_customer c
LEFT JOIN staging.stg_person p ON c.person_id = p.business_entity_id
WHERE c.customer_id IS NOT NULL;

CREATE VIEW staging.vw_stg_product AS
SELECT
    product_id,
    product_name,
    product_number,
    color,
    class,
    product_line,
    standard_cost::NUMERIC(19,4) AS standard_cost,
    list_price::NUMERIC(19,4)    AS list_price,
    product_subcategory_id,
    CASE WHEN sell_end_date IS NULL AND discontinued_date IS NULL THEN TRUE ELSE FALSE END AS is_active,
    sell_start_date,
    sell_end_date,
    modified_date
FROM staging.stg_product;

CREATE VIEW staging.vw_stg_sales AS
SELECT
    d.sales_order_id,
    d.sales_order_detail_id,
    d.order_qty,
    d.product_id,
    d.unit_price,
    d.unit_price_discount,
    d.line_total,
    CASE WHEN r.return_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_returned,
    d.modified_date
FROM staging.stg_sales_order_detail d
LEFT JOIN staging.stg_returns r
    ON d.sales_order_id = r.sales_order_id
    AND d.sales_order_detail_id = r.sales_order_detail_id;

CREATE VIEW staging.vw_sales_summary AS
SELECT
    CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    d.product_id,
    SUM(d.order_qty)   AS total_qty,
    SUM(d.line_total)  AS total_revenue,
    COUNT(DISTINCT h.sales_order_id) AS order_count
FROM staging.stg_sales_order_header h
INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
GROUP BY CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER), d.product_id;
