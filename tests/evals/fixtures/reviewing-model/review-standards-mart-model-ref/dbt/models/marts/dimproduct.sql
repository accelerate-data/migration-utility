{{ config(materialized='table') }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

final as (
    select
        cast(ProductAlternateKey as nvarchar(25)) as product_alternate_key,
        EnglishProductName as english_product_name,
        coalesce(Color, '') as color,
        StandardCost as standard_cost,
        ListPrice as list_price,
        case
            when SellEndDate is null and DiscontinuedDate is null then 'Current'
            when DiscontinuedDate is not null then 'Obsolete'
            else 'Outdated'
        end as status
    from source_product
)

select * from final
