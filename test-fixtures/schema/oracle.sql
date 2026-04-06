-- ==========================================================
-- Kimball DW Fixture — Oracle DDL
-- Self-contained, idempotent: drops and recreates all objects
-- Run as: kimball user in FREEPDB1
-- ==========================================================

-- ----------------------------------------------------------
-- Drop existing objects (reverse dependency order)
-- ----------------------------------------------------------

-- Views
BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_sales_summary';       EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_stg_sales';           EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_stg_product';         EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_stg_customer';        EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-- Facts
BEGIN EXECUTE IMMEDIATE 'DROP TABLE fct_sales_by_channel CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE fct_sales_summary CASCADE CONSTRAINTS PURGE';    EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE fct_sales CASCADE CONSTRAINTS PURGE';            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-- Dimensions
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_order_status CASCADE CONSTRAINTS PURGE';     EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_credit_card CASCADE CONSTRAINTS PURGE';      EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_address CASCADE CONSTRAINTS PURGE';          EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_product_category CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_employee CASCADE CONSTRAINTS PURGE';         EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_date CASCADE CONSTRAINTS PURGE';             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_product CASCADE CONSTRAINTS PURGE';          EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dim_customer CASCADE CONSTRAINTS PURGE';         EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-- Staging
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_returns CASCADE CONSTRAINTS PURGE';            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_employee CASCADE CONSTRAINTS PURGE';           EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_credit_card CASCADE CONSTRAINTS PURGE';        EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_address CASCADE CONSTRAINTS PURGE';            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_sales_order_detail CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_sales_order_header CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_product_category CASCADE CONSTRAINTS PURGE';   EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_product_subcategory CASCADE CONSTRAINTS PURGE';EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_product CASCADE CONSTRAINTS PURGE';            EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_person CASCADE CONSTRAINTS PURGE';             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_customer CASCADE CONSTRAINTS PURGE';           EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-- ==========================================================
-- Staging tables
-- ==========================================================

CREATE TABLE stg_customer (
    customer_id        NUMBER(10)     NOT NULL,
    person_id          NUMBER(10)     NULL,
    store_id           NUMBER(10)     NULL,
    territory_id       NUMBER(10)     NULL,
    account_number     VARCHAR2(20)   NULL,
    modified_date      TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_customer PRIMARY KEY (customer_id)
);

CREATE TABLE stg_person (
    business_entity_id NUMBER(10)     NOT NULL,
    person_type        CHAR(2)        NOT NULL,
    title              VARCHAR2(8)    NULL,
    first_name         VARCHAR2(50)   NOT NULL,
    middle_name        VARCHAR2(50)   NULL,
    last_name          VARCHAR2(50)   NOT NULL,
    suffix             VARCHAR2(10)   NULL,
    email_promotion    NUMBER(10)     DEFAULT 0 NOT NULL,
    modified_date      TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_person PRIMARY KEY (business_entity_id)
);

CREATE TABLE stg_product (
    product_id              NUMBER(10)     NOT NULL,
    product_name            VARCHAR2(100)  NOT NULL,
    product_number          VARCHAR2(25)   NOT NULL,
    make_flag               NUMBER(1)      DEFAULT 1 NOT NULL,
    finished_goods_flag     NUMBER(1)      DEFAULT 1 NOT NULL,
    color                   VARCHAR2(15)   NULL,
    safety_stock_level      NUMBER(5)      NOT NULL,
    reorder_point           NUMBER(5)      NOT NULL,
    standard_cost           NUMBER(19,4)   NOT NULL,
    list_price              NUMBER(19,4)   NOT NULL,
    product_size            VARCHAR2(5)    NULL,
    weight                  NUMBER(8,2)    NULL,
    days_to_manufacture     NUMBER(10)     NOT NULL,
    product_line            CHAR(2)        NULL,
    class                   CHAR(2)        NULL,
    style                   CHAR(2)        NULL,
    product_subcategory_id  NUMBER(10)     NULL,
    product_model_id        NUMBER(10)     NULL,
    sell_start_date         TIMESTAMP      NOT NULL,
    sell_end_date           TIMESTAMP      NULL,
    discontinued_date       TIMESTAMP      NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product PRIMARY KEY (product_id)
);

CREATE TABLE stg_product_subcategory (
    product_subcategory_id  NUMBER(10)     NOT NULL,
    product_category_id     NUMBER(10)     NOT NULL,
    subcategory_name        VARCHAR2(50)   NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product_subcategory PRIMARY KEY (product_subcategory_id)
);

CREATE TABLE stg_product_category (
    product_category_id  NUMBER(10)     NOT NULL,
    category_name        VARCHAR2(50)   NOT NULL,
    modified_date        TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_product_category PRIMARY KEY (product_category_id)
);

CREATE TABLE stg_sales_order_header (
    sales_order_id          NUMBER(10)     NOT NULL,
    revision_number         NUMBER(5)      DEFAULT 0 NOT NULL,
    order_date              TIMESTAMP      NOT NULL,
    due_date                TIMESTAMP      NOT NULL,
    ship_date               TIMESTAMP      NULL,
    status                  NUMBER(5)      DEFAULT 1 NOT NULL,
    online_order_flag       NUMBER(1)      DEFAULT 1 NOT NULL,
    sales_order_number      VARCHAR2(25)   NULL,
    customer_id             NUMBER(10)     NOT NULL,
    sales_person_id         NUMBER(10)     NULL,
    territory_id            NUMBER(10)     NULL,
    bill_to_address_id      NUMBER(10)     NOT NULL,
    ship_to_address_id      NUMBER(10)     NOT NULL,
    ship_method_id          NUMBER(10)     NOT NULL,
    credit_card_id          NUMBER(10)     NULL,
    sub_total               NUMBER(19,4)   DEFAULT 0 NOT NULL,
    tax_amt                 NUMBER(19,4)   DEFAULT 0 NOT NULL,
    freight                 NUMBER(19,4)   DEFAULT 0 NOT NULL,
    total_due               NUMBER(19,4)   NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_sales_order_header PRIMARY KEY (sales_order_id)
);

CREATE TABLE stg_sales_order_detail (
    sales_order_id          NUMBER(10)     NOT NULL,
    sales_order_detail_id   NUMBER(10)     NOT NULL,
    carrier_tracking_number VARCHAR2(25)   NULL,
    order_qty               NUMBER(5)      NOT NULL,
    product_id              NUMBER(10)     NOT NULL,
    special_offer_id        NUMBER(10)     NOT NULL,
    unit_price              NUMBER(19,4)   NOT NULL,
    unit_price_discount     NUMBER(19,4)   DEFAULT 0 NOT NULL,
    line_total              NUMBER(38,6)   NOT NULL,
    modified_date           TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_sales_order_detail PRIMARY KEY (sales_order_id, sales_order_detail_id)
);

CREATE TABLE stg_address (
    address_id        NUMBER(10)     NOT NULL,
    address_line_1    VARCHAR2(60)   NOT NULL,
    address_line_2    VARCHAR2(60)   NULL,
    city              VARCHAR2(30)   NOT NULL,
    state_province_id NUMBER(10)     NOT NULL,
    postal_code       VARCHAR2(15)   NOT NULL,
    modified_date     TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_address PRIMARY KEY (address_id)
);

CREATE TABLE stg_credit_card (
    credit_card_id  NUMBER(10)     NOT NULL,
    card_type       VARCHAR2(50)   NOT NULL,
    card_number     VARCHAR2(25)   NOT NULL,
    exp_month       NUMBER(5)      NOT NULL,
    exp_year        NUMBER(5)      NOT NULL,
    modified_date   TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_credit_card PRIMARY KEY (credit_card_id)
);

CREATE TABLE stg_employee (
    business_entity_id  NUMBER(10)     NOT NULL,
    national_id_number  VARCHAR2(15)   NOT NULL,
    login_id            VARCHAR2(256)  NOT NULL,
    job_title           VARCHAR2(50)   NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              CHAR(1)        NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       NUMBER(1)      DEFAULT 1 NOT NULL,
    vacation_hours      NUMBER(5)      DEFAULT 0 NOT NULL,
    sick_leave_hours    NUMBER(5)      DEFAULT 0 NOT NULL,
    current_flag        NUMBER(1)      DEFAULT 1 NOT NULL,
    first_name          VARCHAR2(50)   NOT NULL,
    last_name           VARCHAR2(50)   NOT NULL,
    modified_date       TIMESTAMP      NOT NULL,
    CONSTRAINT pk_stg_employee PRIMARY KEY (business_entity_id)
);

CREATE TABLE stg_returns (
    return_id              NUMBER(10)     NOT NULL,
    sales_order_id         NUMBER(10)     NOT NULL,
    sales_order_detail_id  NUMBER(10)     NOT NULL,
    return_date            TIMESTAMP      NOT NULL,
    return_qty             NUMBER(5)      NOT NULL,
    return_reason          VARCHAR2(100)  NULL,
    CONSTRAINT pk_stg_returns PRIMARY KEY (return_id)
);

-- ==========================================================
-- Dimension tables
-- ==========================================================

CREATE TABLE dim_customer (
    customer_key     NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id      NUMBER(10)     NOT NULL,
    person_id        NUMBER(10)     NULL,
    store_id         NUMBER(10)     NULL,
    full_name        VARCHAR2(150)  NULL,
    store_name       VARCHAR2(100)  NULL,
    territory_id     NUMBER(10)     NULL,
    valid_from       TIMESTAMP      NOT NULL,
    valid_to         TIMESTAMP      NULL,
    is_current       NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);

CREATE TABLE dim_product (
    product_key             NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_id              NUMBER(10)     NOT NULL,
    product_name            VARCHAR2(100)  NOT NULL,
    product_number          VARCHAR2(25)   NOT NULL,
    color                   VARCHAR2(15)   NULL,
    class                   CHAR(2)        NULL,
    product_line            CHAR(2)        NULL,
    standard_cost           NUMBER(19,4)   NOT NULL,
    list_price              NUMBER(19,4)   NOT NULL,
    product_subcategory     VARCHAR2(50)   NULL,
    product_category        VARCHAR2(50)   NULL,
    sell_start_date         TIMESTAMP      NOT NULL,
    sell_end_date           TIMESTAMP      NULL,
    valid_from              TIMESTAMP      NOT NULL,
    valid_to                TIMESTAMP      NULL,
    is_current              NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
);

CREATE TABLE dim_date (
    date_key               NUMBER(10)     NOT NULL,
    date_day               DATE           NOT NULL,
    day_of_week            NUMBER(5)      NOT NULL,
    day_of_week_name       VARCHAR2(10)   NOT NULL,
    day_of_month           NUMBER(5)      NOT NULL,
    day_of_year            NUMBER(5)      NOT NULL,
    week_of_year           NUMBER(5)      NOT NULL,
    month_number           NUMBER(5)      NOT NULL,
    month_name             VARCHAR2(10)   NOT NULL,
    quarter_number         NUMBER(5)      NOT NULL,
    year_number            NUMBER(10)     NOT NULL,
    is_weekend             NUMBER(1)      NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE TABLE dim_employee (
    employee_key        NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    employee_id         NUMBER(10)     NOT NULL,
    national_id_number  VARCHAR2(15)   NOT NULL,
    first_name          VARCHAR2(50)   NOT NULL,
    last_name           VARCHAR2(50)   NOT NULL,
    job_title           VARCHAR2(50)   NOT NULL,
    birth_date          DATE           NOT NULL,
    gender              CHAR(1)        NOT NULL,
    hire_date           DATE           NOT NULL,
    salaried_flag       NUMBER(1)      DEFAULT 1 NOT NULL,
    current_flag        NUMBER(1)      DEFAULT 1 NOT NULL,
    valid_from          TIMESTAMP      NOT NULL,
    valid_to            TIMESTAMP      NULL,
    is_current          NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key)
);

CREATE TABLE dim_product_category (
    product_category_key  NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_category_id   NUMBER(10)     NOT NULL,
    category_name         VARCHAR2(50)   NOT NULL,
    valid_from            TIMESTAMP      NOT NULL,
    valid_to              TIMESTAMP      NULL,
    is_current            NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_product_category PRIMARY KEY (product_category_key)
);

CREATE TABLE dim_address (
    address_key       NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    address_id        NUMBER(10)     NOT NULL,
    address_line_1    VARCHAR2(60)   NOT NULL,
    city              VARCHAR2(30)   NOT NULL,
    state_province_id NUMBER(10)     NOT NULL,
    postal_code       VARCHAR2(15)   NOT NULL,
    valid_from        TIMESTAMP      NOT NULL,
    valid_to          TIMESTAMP      NULL,
    is_current        NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_address PRIMARY KEY (address_key)
);

CREATE TABLE dim_credit_card (
    credit_card_key  NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    credit_card_id   NUMBER(10)     NOT NULL,
    card_type        VARCHAR2(50)   NOT NULL,
    exp_month        NUMBER(5)      NOT NULL,
    exp_year         NUMBER(5)      NOT NULL,
    valid_from       TIMESTAMP      NOT NULL,
    valid_to         TIMESTAMP      NULL,
    is_current       NUMBER(1)      DEFAULT 1 NOT NULL,
    CONSTRAINT pk_dim_credit_card PRIMARY KEY (credit_card_key)
);

CREATE TABLE dim_order_status (
    order_status_key   NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    order_status       NUMBER(5)      NOT NULL,
    order_status_name  VARCHAR2(20)   NOT NULL,
    CONSTRAINT pk_dim_order_status PRIMARY KEY (order_status_key)
);

-- ==========================================================
-- Fact tables
-- ==========================================================

CREATE TABLE fct_sales (
    sales_key              NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    sales_order_id         NUMBER(10)     NOT NULL,
    sales_order_detail_id  NUMBER(10)     NOT NULL,
    customer_key           NUMBER(10)     NOT NULL,
    product_key            NUMBER(10)     NOT NULL,
    date_key               NUMBER(10)     NOT NULL,
    address_key            NUMBER(10)     NULL,
    credit_card_key        NUMBER(10)     NULL,
    order_status_key       NUMBER(10)     NOT NULL,
    order_qty              NUMBER(5)      NOT NULL,
    unit_price             NUMBER(19,4)   NOT NULL,
    unit_price_discount    NUMBER(19,4)   DEFAULT 0 NOT NULL,
    line_total             NUMBER(38,6)   NOT NULL,
    CONSTRAINT pk_fct_sales PRIMARY KEY (sales_key)
);

CREATE TABLE fct_sales_summary (
    sales_summary_key  NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    date_key           NUMBER(10)     NOT NULL,
    product_key        NUMBER(10)     NOT NULL,
    total_qty          NUMBER(10)     NOT NULL,
    total_revenue      NUMBER(38,6)   NOT NULL,
    order_count        NUMBER(10)     NOT NULL,
    CONSTRAINT pk_fct_sales_summary PRIMARY KEY (sales_summary_key)
);

CREATE TABLE fct_sales_by_channel (
    sales_channel_key  NUMBER(10) GENERATED ALWAYS AS IDENTITY NOT NULL,
    date_key           NUMBER(10)     NOT NULL,
    channel            VARCHAR2(10)   NOT NULL,
    total_qty          NUMBER(10)     NOT NULL,
    total_revenue      NUMBER(38,6)   NOT NULL,
    order_count        NUMBER(10)     NOT NULL,
    CONSTRAINT pk_fct_sales_by_channel PRIMARY KEY (sales_channel_key)
);

-- ==========================================================
-- Views
-- ==========================================================

CREATE OR REPLACE VIEW vw_stg_customer AS
SELECT
    c.customer_id,
    c.person_id,
    c.store_id,
    c.territory_id,
    p.first_name || ' ' || NVL(p.middle_name || ' ', '') || p.last_name AS full_name,
    c.modified_date
FROM stg_customer c
LEFT JOIN stg_person p ON c.person_id = p.business_entity_id
WHERE c.customer_id IS NOT NULL;

CREATE OR REPLACE VIEW vw_stg_product AS
SELECT
    product_id,
    product_name,
    product_number,
    color,
    class,
    product_line,
    CAST(standard_cost AS NUMBER(19,4)) AS standard_cost,
    CAST(list_price AS NUMBER(19,4))    AS list_price,
    product_subcategory_id,
    CASE WHEN sell_end_date IS NULL AND discontinued_date IS NULL THEN 1 ELSE 0 END AS is_active,
    sell_start_date,
    sell_end_date,
    modified_date
FROM stg_product;

CREATE OR REPLACE VIEW vw_stg_sales AS
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
FROM stg_sales_order_detail d
LEFT JOIN stg_returns r
    ON d.sales_order_id = r.sales_order_id
    AND d.sales_order_detail_id = r.sales_order_detail_id;

CREATE OR REPLACE VIEW vw_sales_summary AS
SELECT
    TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
    d.product_id,
    SUM(d.order_qty)   AS total_qty,
    SUM(d.line_total)  AS total_revenue,
    COUNT(DISTINCT h.sales_order_id) AS order_count
FROM stg_sales_order_header h
INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
GROUP BY TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')), d.product_id;
