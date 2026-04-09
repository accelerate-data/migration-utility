{{ config(
    materialized='table'
) }}

with source_product as (
    select * from {{ ref('stg_product') }}
),

final as (
    select
        cast(ProductID as NVARCHAR(25)) as ProductAlternateKey,
        ProductName as EnglishProductName
    from source_product
)

select * from final
