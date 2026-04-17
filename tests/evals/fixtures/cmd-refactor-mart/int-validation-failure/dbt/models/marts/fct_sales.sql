select
  order_id,
  customer_id,
  amount
from {{ source('silver', 'sales_orders') }}
