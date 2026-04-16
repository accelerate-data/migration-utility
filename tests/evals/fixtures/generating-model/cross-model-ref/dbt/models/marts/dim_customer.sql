{{ config(materialized='table') }}

with source_customer as (
    select * from {{ ref('stg_bronze__customer') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} as CustomerKey,
        cast(CustomerID as nvarchar(15)) as CustomerAlternateKey,
        FirstName,
        LastName
    from source_customer
)

select * from final
