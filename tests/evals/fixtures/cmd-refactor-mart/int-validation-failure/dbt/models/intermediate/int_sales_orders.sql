-- EVAL_VALIDATION_FAIL: simulate dbt validation failure after int rewrite
select
  order_id,
  customer_id,
  order_date,
  amount
from {{ source('bronze', 'orders') }}
