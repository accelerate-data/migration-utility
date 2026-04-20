CREATE TABLE sales.fact_sales_orders (
  sales_order_key integer NOT NULL,
  order_number varchar(30) NOT NULL,
  customer_key integer NOT NULL,
  product_key integer NOT NULL,
  sales_rep_key integer NOT NULL,
  order_date_key integer NOT NULL,
  gross_sales_amount numeric(18, 2) NOT NULL,
  discount_amount numeric(18, 2) NOT NULL,
  net_sales_amount numeric(18, 2) NOT NULL
);
