{{ config(materialized='table') }}

with target_table as (
    select * from {{ source('silver', 'updatejointarget') }}
),

with source_product as (
    SELECT *
    from {{ source('bronze', 'product') }}
),

updated_rows as (
    select
        tgt.ProductAlternateKey,
        coalesce(src.ProductName, tgt.EnglishProductName) as EnglishProductName,
        case
            when src.ProductID is not null then current_timestamp()
            else tgt.LastSeenDate
        end as LastSeenDate,
        {{ invocation_id }} as _dbt_run_id,
        current_timestamp() as _loaded_at
    from target_table as tgt
    left join source_product as src
        on tgt.ProductAlternateKey = cast(src.ProductID as nvarchar(25))
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        LastSeenDate,
        _dbt_run_id,
        _loaded_at
    from updated_rows
)

select *
from final
