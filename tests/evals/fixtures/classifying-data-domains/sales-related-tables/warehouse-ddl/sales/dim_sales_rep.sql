CREATE TABLE sales.dim_sales_rep (
  sales_rep_key integer NOT NULL,
  sales_rep_id varchar(30) NOT NULL,
  sales_rep_name varchar(200) NOT NULL,
  sales_region varchar(100) NOT NULL,
  effective_start_date date NOT NULL,
  effective_end_date date NULL,
  is_current bit NOT NULL
);
