select
  customer_id,
  sum(amount) as total_amount
from {{ source('bronze', 'orders') }}
group by customer_id
