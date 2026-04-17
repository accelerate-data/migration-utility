with

source as (

    select * from {{ source('bronze', 'person') }}

),

final as (

    select * from source

)

select * from final
