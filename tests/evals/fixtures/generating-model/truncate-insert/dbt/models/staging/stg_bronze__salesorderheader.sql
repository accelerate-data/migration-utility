with

source as (

    select * from {{ source('bronze', 'salesorderheader') }}

),

final as (

    select * from source

)

select * from final
