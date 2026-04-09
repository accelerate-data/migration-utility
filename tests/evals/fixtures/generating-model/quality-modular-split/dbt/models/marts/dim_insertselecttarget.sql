{{ config(
    materialized='table'
) }}

with source_product as (
    select * from {{ ref('stg_insertselecttarget') }}
),

final as (
    select
        *,
        getdate() as _loaded_at
    from source_product
)

select * from final
