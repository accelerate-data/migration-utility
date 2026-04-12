{{ config(materialized='table') }}

{#
  Source procedure uses IF/ELSE on AVG(ListPrice) to choose between
  premium (>100) and standard paths. In dbt, both paths collapse into
  a single SELECT with a CASE expression — no imperative branching.
#}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

avg_price as (
    select avg(ListPrice) as avg_list_price
    from source_product
),

categorized as (
    select
        cast(p.ProductID as nvarchar(25)) as ProductAlternateKey,
        p.ProductName                     as EnglishProductName,
        case
            when a.avg_list_price > 100 and p.ListPrice > 100 then 'Premium'
            when a.avg_list_price > 100 and p.ListPrice <= 100 then null
            else 'Standard'
        end as PriceCategory
    from source_product p
    cross join avg_price a
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        PriceCategory
    from categorized
    where PriceCategory is not null
)

select * from final
