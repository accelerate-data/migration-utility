{{ config(materialized='table') }}

with active_customers as (
    select * from {{ ref('stg_vw_activecustomers') }}
),

final as (
    select
        CustomerID,
        concat(FirstName, ' ', LastName) as FullName
    from active_customers
)

select * from final
