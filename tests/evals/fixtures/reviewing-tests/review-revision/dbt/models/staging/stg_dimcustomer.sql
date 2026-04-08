{{ config(materialized='incremental') }}

with source_customer as (
    select *
    from {{ source('bronze', 'customer') }}
),

final as (
    select
        cast(CustomerID as varchar) as CustomerAlternateKey
    from source_customer
)

select *
from final
