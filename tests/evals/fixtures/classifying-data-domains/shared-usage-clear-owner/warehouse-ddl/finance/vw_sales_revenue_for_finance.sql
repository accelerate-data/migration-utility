CREATE VIEW finance.vw_sales_revenue_for_finance AS
SELECT
  sales_order_key,
  order_date_key,
  net_sales_amount
FROM sales.fact_sales_orders
WHERE net_sales_amount <> 0;
