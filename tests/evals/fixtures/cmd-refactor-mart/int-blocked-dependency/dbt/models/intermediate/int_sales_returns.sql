select
  return_id,
  order_id,
  customer_id,
  return_date,
  amount
from {{ source('bronze', 'returns') }}
