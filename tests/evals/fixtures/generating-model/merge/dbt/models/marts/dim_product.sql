{{ config(
    materialized='incremental',
    unique_id='ProductAlternateKey',
) }}

with staged_product as (
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
    from {{ ref('stg_dimproduct') }}
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
        {{ invocation_id }} as _dbt_run_id,
    from staged_product
)

select * from final
