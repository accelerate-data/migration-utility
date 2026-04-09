{{ config(materialized='ephemeral') }}

select
    cast(ProductID as nvarchar(25)) as ProductAlternateKey,
    ProductName as EnglishProductName,
    StandardCost,
    ListPrice,
    Size,
    ProductLine,
    Class,
    Style,
    SellStartDate as StartDate,
    SellEndDate as EndDate
from {{ source('bronze', 'product') }}
