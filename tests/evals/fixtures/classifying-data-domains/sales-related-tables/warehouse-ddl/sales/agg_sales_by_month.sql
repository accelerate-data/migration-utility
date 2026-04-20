CREATE TABLE sales.agg_sales_by_month (
  month_key integer NOT NULL,
  sales_region_key integer NOT NULL,
  order_count integer NOT NULL,
  net_sales_amount numeric(18, 2) NOT NULL
);
