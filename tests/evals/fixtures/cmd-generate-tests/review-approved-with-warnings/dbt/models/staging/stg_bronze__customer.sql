with source as (

    select * from {{ source('bronze', 'customer') }}

)

select * from source
