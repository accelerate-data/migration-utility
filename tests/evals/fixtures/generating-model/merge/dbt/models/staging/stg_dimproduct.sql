{{ config(
    materialized='view',
) }}

with source_product as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName,
        StandardCost,
        ListPrice,
        isnull(Color, '') as Color,
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
        end as Status,
    from {{ source('bronze', 'product') }}
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
        Status,
    from source_product
)

select * from final
