with

source as (

    select * from {{ source('bronze', 'customerraw') }}

),

final as (

    select * from source

)

select * from final
