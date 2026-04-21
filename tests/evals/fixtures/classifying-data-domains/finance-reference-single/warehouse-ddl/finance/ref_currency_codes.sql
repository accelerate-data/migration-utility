CREATE TABLE finance.ref_currency_codes (
  currency_code char(3) NOT NULL,
  currency_name varchar(100) NOT NULL,
  minor_unit_count integer NOT NULL,
  iso_numeric_code varchar(3) NOT NULL
);
