with

source as (

    select * from {{ source('bronze', 'product') }}

),

final as (

    select * from source

)

select * from final
