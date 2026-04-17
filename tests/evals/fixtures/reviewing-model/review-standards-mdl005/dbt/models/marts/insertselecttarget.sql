with source_product as (
    select * from {{ ref('stg_bronze__product') }}
),

prepared_product as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName                    as EnglishProductName
    from source_product
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName
    from prepared_product
)

select * from final
