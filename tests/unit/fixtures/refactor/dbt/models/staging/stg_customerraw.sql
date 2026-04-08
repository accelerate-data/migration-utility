{{ config(materialized='ephemeral') }}

select
    customer_id,
    first_name,
    last_name,
    is_active
from {{ source('bronze', 'CustomerRaw') }}
