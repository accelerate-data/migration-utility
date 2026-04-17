with

source as (

    select * from {{ source('bronze', 'employee') }}

),

final as (

    select * from source

)

select * from final
