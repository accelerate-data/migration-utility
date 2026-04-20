CREATE TABLE customer.dim_customer (
  customer_key integer NOT NULL,
  customer_id varchar(30) NOT NULL,
  customer_name varchar(200) NOT NULL,
  customer_segment varchar(100) NULL,
  effective_start_date date NOT NULL,
  effective_end_date date NULL,
  is_current bit NOT NULL
);
