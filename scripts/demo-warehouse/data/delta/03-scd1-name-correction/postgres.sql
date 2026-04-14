-- Delta 03: Existing customer with corrected name (SCD Type 1 — overwrite)
-- Simulates staging refresh: person business_entity_id=1 reappears with corrected last name.
-- Procedure should UPDATE dim_customer.full_name in place (no new row).
--
-- Expected: customer with person_id=1 still has 1 dim row, full_name updated

DELETE FROM staging.stg_person WHERE business_entity_id = 1;

INSERT INTO staging.stg_person (business_entity_id, person_type, title, first_name, middle_name, last_name, suffix, email_promotion, modified_date)
VALUES (1, 'EM', NULL, 'Ken', 'J', 'Sanchez-Corrected', NULL, 0, '2014-09-01 00:00:00');
