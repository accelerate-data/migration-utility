-- Delta 02: Existing customer with changed address (SCD Type 2 — new row)
-- Simulates staging refresh: address_id 1 reappears with a different city.
-- Procedure should expire current dim_address row (set valid_to, is_current=0)
-- and insert a new dim_address row with the updated city.
--
-- Expected dim_address assertions after procedure runs:
--   address_id=1 should have 2 rows: one expired, one current
USE KimballFixture;
GO

-- Remove old staging row and replace with changed version
DELETE FROM staging.stg_address WHERE address_id = 1;

INSERT INTO staging.stg_address (address_id, address_line_1, address_line_2, city, state_province_id, postal_code, modified_date)
VALUES (1, N'1970 Napa Ct.', NULL, N'New Delta City', 79, N'98011', '2014-08-01T00:00:00.000');
GO
