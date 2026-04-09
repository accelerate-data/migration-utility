{{ config(
    materialized='table'
) }}

with source_product as (
    select * from {{ ref('stg_product') }}
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName
    from source_product
)

select * from final
