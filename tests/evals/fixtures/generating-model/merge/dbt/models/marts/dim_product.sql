{{ config(
    materialized='table'
) }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

prepared_product as (
    select
        cast(ProductID as string) as ProductAlternateKey,
        ProductName as EnglishProductName,
        StandardCost,
        ListPrice,
        coalesce(Color, '') as Color,
        Size,
        ProductLine,
        Class,
        Style,
        SellStartDate as StartDate,
        SellEndDate as EndDate,
        case
            when DiscontinuedDate is not null then 'Obsolete'
            when SellEndDate is not null then 'Outdated'
            else 'Current'
        end as Status
    from source_product
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
    from prepared_product
)

select * from final
