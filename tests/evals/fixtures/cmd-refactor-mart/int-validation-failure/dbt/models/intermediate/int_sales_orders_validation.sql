-- EVAL_VALIDATION_FAIL: simulate validation failure after int_sales_orders is rewritten
select
  order_id
from {{ ref('int_sales_orders') }}
