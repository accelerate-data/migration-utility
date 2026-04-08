{{ config(materialized='table') }}

with product as (
    select * from {{ ref('stg_product') }}
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName
    from product
)

select * from final
