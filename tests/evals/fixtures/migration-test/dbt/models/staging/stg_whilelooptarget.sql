{{ config(
    materialized='table'
) }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

renamed as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName                     as EnglishProductName
    from source_product
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName
    from renamed
)

select * from final
