{{ config(
    materialized='ephemeral'
) }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
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
        EnglishProductName,
        cast('{{ invocation_id }}' as nvarchar(max)) as _dbt_run_id
    from prepared_product
)

select * from final
