{{ config(materialized='view') }}

with source_customerraw as (
    select * from {{ source('bronze', 'customerraw') }}
),

final as (
    select
        CustomerID,
        FirstName,
        LastName
    from source_customerraw
    where IsActive = 1
)

select * from final
