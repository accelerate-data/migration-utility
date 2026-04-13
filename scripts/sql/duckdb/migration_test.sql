create schema if not exists bronze;
create schema if not exists silver;

drop view if exists silver.v_currency_names;
drop table if exists silver.DimCurrency;
drop table if exists silver.DimProduct;
drop table if exists bronze.Currency;
drop table if exists bronze.Product;
drop table if exists bronze.Promotion;
drop table if exists bronze.Customer;
drop table if exists bronze.Person;
drop table if exists bronze.SalesOrderHeader;
drop table if exists bronze.SalesOrderDetail;
drop table if exists bronze.SalesTerritory;
drop table if exists migrationtest_fixture_info;

create table migrationtest_fixture_info (
  fixture_name varchar not null,
  backend varchar not null
);

insert into migrationtest_fixture_info (fixture_name, backend)
values ('MigrationTest', 'duckdb');

create table bronze.Currency (
  CurrencyCode varchar not null,
  CurrencyName varchar not null,
  ModifiedDate timestamp
);

insert into bronze.Currency values
  ('USD', 'US Dollar', timestamp '2024-01-01 00:00:00'),
  ('EUR', 'Euro', timestamp '2024-01-01 00:00:00');

create table bronze.Product (
  ProductID bigint not null,
  ProductName varchar not null,
  Color varchar,
  StandardCost decimal(19,4),
  ListPrice decimal(19,4),
  SellStartDate date
);

insert into bronze.Product values
  (1, 'Widget A', 'Red', 10.00, 20.00, date '2024-01-01'),
  (2, 'Widget B', 'Blue', 15.00, 30.00, date '2024-01-01');

create table bronze.Promotion (
  PromotionID bigint not null,
  Description varchar,
  DiscountPct double,
  PromotionType varchar,
  PromotionCategory varchar,
  StartDate date
);

insert into bronze.Promotion values
  (1, 'Summer Sale', 0.10, 'Discount', 'Seasonal', date '2024-06-01');

create table bronze.Customer (
  CustomerID bigint not null,
  PersonID bigint,
  TerritoryID bigint
);

create table bronze.Person (
  BusinessEntityID bigint not null,
  FirstName varchar,
  LastName varchar,
  EmailPromotion bigint
);

create table bronze.SalesOrderHeader (
  SalesOrderID bigint not null,
  SalesOrderNumber varchar,
  CustomerID bigint,
  OrderDate date,
  TerritoryID bigint,
  TaxAmt decimal(19,4),
  Freight decimal(19,4)
);

create table bronze.SalesOrderDetail (
  SalesOrderID bigint not null,
  SalesOrderDetailID bigint not null,
  OrderQty bigint,
  ProductID bigint,
  UnitPrice decimal(19,4),
  LineTotal decimal(19,4)
);

create table bronze.SalesTerritory (
  TerritoryID bigint not null,
  TerritoryName varchar,
  CountryRegionCode varchar,
  TerritoryGroup varchar,
  SalesYTD decimal(19,4)
);

create table silver.DimCurrency (
  CurrencyKey bigint,
  CurrencyAlternateKey varchar,
  CurrencyName varchar
);

create table silver.DimProduct (
  ProductKey bigint,
  ProductAlternateKey varchar,
  EnglishProductName varchar,
  StandardCost decimal(19,4),
  ListPrice decimal(19,4),
  Color varchar
);

create view silver.v_currency_names as
select CurrencyCode, CurrencyName
from bronze.Currency;
