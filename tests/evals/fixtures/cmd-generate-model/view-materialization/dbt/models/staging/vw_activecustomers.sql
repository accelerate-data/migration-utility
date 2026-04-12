{{ config(
    materialized='view'
) }}

with source_customerraw as (
    select * from {{ source('bronze', 'customerraw') }}
),

active_customers as (
    select
        CustomerID,
        FirstName,
        LastName
    from source_customerraw
    where IsActive = 1
),

final as (
    select
        CustomerID,
        FirstName,
        LastName
    from active_customers
)

select * from final
