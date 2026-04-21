CREATE TABLE staging.stg_sales_orders (
  source_system varchar(50) NOT NULL,
  source_order_id varchar(50) NOT NULL,
  source_customer_id varchar(50) NOT NULL,
  source_product_id varchar(50) NOT NULL,
  order_status varchar(50) NULL,
  order_total numeric(18, 2) NULL,
  extracted_at datetime2 NOT NULL
);
