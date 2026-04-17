with source_product as (
    select * from {{ ref('stg_bronze__product') }}
),

source_currency as (
    select * from {{ source('bronze', 'currency') }}
),

prepared_product as (
    select
        cast(ProductID as nvarchar(25)) as ProductAlternateKey,
        ProductName as EnglishProductName,
        '{{ invocation_id }}' as _dbt_run_id,
        {{ current_timestamp() }} as _loaded_at
    from source_product
    where exists (
        select 1
        from source_currency
        where CurrencyCode = 'USD'
    )
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        _dbt_run_id,
        _loaded_at
    from prepared_product
)

select * from final
