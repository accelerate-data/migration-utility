-- EVAL_VALIDATION_FAIL: simulate dbt validation failure after staging rewire
select
  return_id,
  order_id,
  return_date
from {{ ref('stg_bronze__returns') }}
