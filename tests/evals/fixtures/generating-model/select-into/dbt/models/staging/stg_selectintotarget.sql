{{ config(
    materialized = 'table',
    schema = 'silver',
    alias = 'SelectIntoTarget'
) }}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

final as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName,
        {{ invocation_id }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from source_product
)

select * from final
