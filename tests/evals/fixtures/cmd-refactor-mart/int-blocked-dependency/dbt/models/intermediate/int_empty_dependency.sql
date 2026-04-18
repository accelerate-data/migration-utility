select
  order_id,
  customer_id,
  order_date
from {{ source('bronze', 'orders') }}
