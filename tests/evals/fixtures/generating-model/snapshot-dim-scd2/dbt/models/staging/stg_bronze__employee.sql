with source as (

    select * from {{ source('bronze', 'employee') }}

)

select * from source
