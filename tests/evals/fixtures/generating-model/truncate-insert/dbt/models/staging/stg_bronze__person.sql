with source as (

    select * from {{ source('bronze', 'person') }}

)

select * from source
