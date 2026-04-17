with

source as (

    select * from {{ source('bronze', 'customer') }}

),

final as (

    select * from source

)

select * from final
