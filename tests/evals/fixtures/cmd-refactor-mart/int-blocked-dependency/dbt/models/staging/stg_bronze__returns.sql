select
  return_id,
  order_id,
  return_date
from {{ source('bronze', 'returns') }}
