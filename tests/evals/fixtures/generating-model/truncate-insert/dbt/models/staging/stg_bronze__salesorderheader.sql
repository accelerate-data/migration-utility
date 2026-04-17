with source as (

    select * from {{ source('bronze', 'salesorderheader') }}

)

select * from source
