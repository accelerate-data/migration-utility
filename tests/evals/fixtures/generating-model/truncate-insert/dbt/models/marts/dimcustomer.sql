{{ config(materialized='table') }}

with source_customer as (
    select * from {{ ref('stg_bronze__customer') }}
),

source_person as (
    select * from {{ ref('stg_bronze__person') }}
),

source_orders as (
    select * from {{ ref('stg_bronze__salesorderheader') }}
),

customer_person as (
    select
        cast(c.CustomerID as nvarchar(15)) as CustomerAlternateKey,
        p.FirstName,
        p.MiddleName,
        p.LastName,
        p.Title,
        null as Gender,
        null as MaritalStatus,
        p.EmailPromotion,
        c.CustomerID
    from source_customer as c
    inner join source_person as p
        on c.PersonID = p.BusinessEntityID
),

order_dates as (
    select
        CustomerID,
        min(OrderDate) as MinOrderDate
    from source_orders
    group by CustomerID
),

final as (
    select
        cp.CustomerAlternateKey,
        cp.FirstName,
        cp.MiddleName,
        cp.LastName,
        cp.Title,
        cp.Gender,
        cp.MaritalStatus,
        cp.EmailPromotion,
        cast(od.MinOrderDate as date) as DateFirstPurchase
    from customer_person as cp
    left join order_dates as od
        on cp.CustomerID = od.CustomerID
)

select * from final
