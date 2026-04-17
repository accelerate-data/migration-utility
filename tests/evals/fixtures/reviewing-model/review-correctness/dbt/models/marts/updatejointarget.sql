with source_product as (
    select *
    from {{ ref('stg_bronze__product') }}
),

product_keyed as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName,
        current_timestamp() as LastSeenDate,
        '{{ invocation_id }}' as _dbt_run_id,
        {{ current_timestamp() }} as _loaded_at,
    from source_product
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        LastSeenDate,
        _dbt_run_id,
        _loaded_at,
    from product_keyed
)

select *
from final
