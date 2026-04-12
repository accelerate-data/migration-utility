{{
  config(
    materialized = 'table',
    meta = {
      'generated_by': 'migrate-util',
      'migration': 'dim_scd1'
    }
  )
}}

with source_product as (
    select * from {{ source('bronze', 'product') }}
),

prepared_product as (
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
        Status,
        {{ invocation_id() }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from prepared_product
)

select * from final
