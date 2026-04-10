{{ config(
    materialized='incremental',
    unique_key='ProductAlternateKey'
) }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        LastSeenDate
    from source_product
)

select * from final
