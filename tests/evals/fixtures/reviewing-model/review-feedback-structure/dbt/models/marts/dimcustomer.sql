{{ config(materialized='incremental') }}

with source_customer as (
    select *
    from {{ ref('stg_bronze__customer') }}
),

final as (
    select
        cast(CustomerID as varchar) as CustomerAlternateKey
    from source_customer
)

select *
from final
