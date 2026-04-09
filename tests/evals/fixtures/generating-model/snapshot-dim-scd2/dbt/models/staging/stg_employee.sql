{{ config(materialized='ephemeral') }}

select * from {{ source('bronze', 'employee') }}
