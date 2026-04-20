CREATE TABLE customer.customer_address_history (
  customer_id varchar(30) NOT NULL,
  address_line_1 varchar(200) NOT NULL,
  city varchar(100) NOT NULL,
  postal_code varchar(20) NOT NULL,
  effective_start_date date NOT NULL,
  effective_end_date date NULL
);
