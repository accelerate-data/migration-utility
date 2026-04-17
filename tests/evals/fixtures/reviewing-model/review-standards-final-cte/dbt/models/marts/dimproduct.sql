with source_product as (
    select *
    from {{ ref('stg_bronze__product') }}
),

prepared_product as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
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

output as (
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

select *
from output
