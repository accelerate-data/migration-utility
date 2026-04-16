with source as (

    select * from {{ source('bronze', 'customerraw') }}

)

select * from source
