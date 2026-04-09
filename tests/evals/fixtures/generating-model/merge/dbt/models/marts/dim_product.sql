{{ config(
    materialized='table'
) }}

with stg_dimproduct as (
    select *
    from {{ ref('stg_dimproduct') }}
),

source_product as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        Color,
        DiscontinuedDate,
        SellEndDate
    from {{ source('bronze', 'product') }}
),

enrich_product as (
    select
        stg.ProductAlternateKey,
        stg.EnglishProductName,
        stg.StandardCost,
        stg.ListPrice,
        coalesce(src.Color, '') as Color,
        stg.Size,
        stg.ProductLine,
        stg.Class,
        stg.Style,
        stg.StartDate,
        stg.EndDate,
        case
            when src.DiscontinuedDate is not null then 'Obsolete'
            when src.SellEndDate is not null then 'Outdated'
            else 'Current'
        end as Status
    from stg_dimproduct stg
    join source_product src on stg.ProductAlternateKey = src.ProductAlternateKey
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        StandardCost,
        ListPrice,
        Color,
        Size,
        ProductLine,
        Class,
        Style,
        StartDate,
        EndDate,
        Status
    from enrich_product
)

select * from final
