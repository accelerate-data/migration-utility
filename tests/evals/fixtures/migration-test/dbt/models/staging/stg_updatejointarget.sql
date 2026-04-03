{{ config(materialized='table') }}

with source_product as (
    select *
    from {{ source('bronze', 'product') }}
),

product_keyed as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName,
        current_timestamp() as LastSeenDate
    from source_product
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        LastSeenDate
    from product_keyed
)

select *
from final
