with source as (

    select * from {{ source('bronze', 'product') }}

)

select * from source
