{{ config(materialized='table') }}

{#
  Source procedure usp_load_DimCurrency uses dynamic sp_executesql to
  build and execute the INSERT statement. The SQL is not statically
  visible, so this model is a graceful placeholder noting the dynamic
  SQL limitation. The recovered SQL (from enrichment) is:
    INSERT INTO silver.DimCurrency (CurrencyAlternateKey, CurrencyName)
    SELECT CurrencyCode, CurrencyName FROM MigrationTest.bronze_currency
#}

with source_currency as (
    select * from {{ source('bronze', 'currency') }}
),

final as (
    select
        CurrencyCode  as CurrencyAlternateKey,
        CurrencyName
    from source_currency
)

select * from final
