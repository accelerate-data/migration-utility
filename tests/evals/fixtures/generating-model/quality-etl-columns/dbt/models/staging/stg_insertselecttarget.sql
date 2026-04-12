{{
    config(
        materialized='table'
    )
}}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

final as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName
    from source_product
)

select * from final
