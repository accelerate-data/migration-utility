{{ config(materialized='table') }}

with src_customer as (
    select * from {{ ref('stg_customerraw') }}
),
src_sales as (
    select * from {{ ref('stg_salesraw') }}
),
final as (
    select
        c.customer_id,
        s.amount
    from src_customer c
    join src_sales s on c.customer_id = s.customer_id
)
select * from final
