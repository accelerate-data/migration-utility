with source as (

    select * from {{ source('bronze', 'salesorder') }}

)

select * from source
