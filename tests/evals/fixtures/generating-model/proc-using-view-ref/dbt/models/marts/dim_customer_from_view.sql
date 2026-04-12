{{ config(materialized='table') }}

with active_customers as (
    select * from {{ source('silver', 'vw_activecustomers') }}
),

final as (
    select
        CustomerID,
        concat(FirstName, ' ', LastName) as FullName
    from active_customers
)

select * from final
