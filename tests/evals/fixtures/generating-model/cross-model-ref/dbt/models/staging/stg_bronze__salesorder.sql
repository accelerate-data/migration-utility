with

source as (

    select * from {{ source('bronze', 'salesorder') }}

),

final as (

    select * from source

)

select * from final
