-- Delta 02: Existing customer with changed address (SCD Type 2 — new row)
-- Simulates staging refresh: address_id 1 reappears with a different city.
-- Procedure should expire current dim_address row and insert new one.
--
-- Expected: address_id=1 has 2 dim_address rows after procedure runs

DELETE FROM stg_address WHERE address_id = 1;

INSERT INTO stg_address (address_id, address_line_1, address_line_2, city, state_province_id, postal_code, modified_date)
VALUES (1, '1970 Napa Ct.', NULL, 'New Delta City', 79, '98011', TO_TIMESTAMP('2014-08-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));

COMMIT;
