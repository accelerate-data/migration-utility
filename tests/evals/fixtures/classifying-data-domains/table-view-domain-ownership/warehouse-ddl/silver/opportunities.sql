CREATE TABLE silver.opportunities (
  opportunity_id integer NOT NULL,
  account_id integer NOT NULL,
  sales_rep_id integer NOT NULL,
  stage_name varchar(100) NOT NULL,
  amount numeric(18, 2) NOT NULL,
  created_at datetime2 NOT NULL
);
