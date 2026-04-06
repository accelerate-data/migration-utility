-- Delta 03: Existing customer with corrected name (SCD Type 1 — overwrite)
-- Simulates staging refresh: person business_entity_id=1 reappears with corrected last name.
-- Procedure should UPDATE dim_customer.full_name in place (no new row).
--
-- Expected dim_customer assertions after procedure runs:
--   customer with person_id=1 still has exactly 1 dim row, but full_name is updated
USE KimballFixture;
GO

-- Replace staging person row with corrected name
DELETE FROM staging.stg_person WHERE business_entity_id = 1;

INSERT INTO staging.stg_person (business_entity_id, person_type, title, first_name, middle_name, last_name, suffix, email_promotion, modified_date)
VALUES (1, N'EM', NULL, N'Ken', N'J', N'Sanchez-Corrected', NULL, 0, '2014-09-01T00:00:00.000');
GO
