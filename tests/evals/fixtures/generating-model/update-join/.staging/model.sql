{{ config(
    materialized='table'
) }}

with product as (
    select * from {{ ref('stg_updatejointarget') }}
),

final as (
    select
        ProductAlternateKey,
        EnglishProductName,
        LastSeenDate
    from product
)

select * from final
