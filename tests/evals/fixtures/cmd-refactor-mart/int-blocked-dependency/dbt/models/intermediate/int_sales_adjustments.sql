select
  order_id,
  customer_id,
  adjustment_amount
from {{ source('bronze', 'order_adjustments') }}
