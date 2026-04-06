-- Delta 05: Duplicate rows in staging (dedup in MERGE)
-- Two identical order header rows appear in staging with the same PK.
-- Since stg_sales_order_header has a PK, we simulate this with a temp
-- staging table that the procedure would read from, or by inserting into
-- a separate dedup_staging table.
--
-- For fixture purposes: we insert a second order detail row for the same
-- (sales_order_id, sales_order_detail_id) combination after removing the PK
-- constraint temporarily. The MERGE procedure must dedup before inserting
-- into fct_sales.
--
-- Expected fct_sales assertions after procedure runs:
--   Only 1 fct_sales row for (sales_order_id=99902, sales_order_detail_id=1)
--   despite 2 staging rows
USE KimballFixture;
GO

-- Insert the "correct" order first
INSERT INTO staging.stg_sales_order_header (sales_order_id, revision_number, order_date, due_date, ship_date, status, online_order_flag, sales_order_number, customer_id, sales_person_id, territory_id, bill_to_address_id, ship_to_address_id, ship_method_id, credit_card_id, sub_total, tax_amt, freight, total_due, modified_date)
VALUES (99902, 0, '2014-01-10T00:00:00.000', '2014-01-20T00:00:00.000', '2014-01-15T00:00:00.000', 5, 1, N'SO99902', 1, NULL, 1, 1, 1, 1, NULL, 99.9800, 8.0000, 2.5000, 110.4800, '2014-11-01T00:00:00.000');

-- Insert first detail row
INSERT INTO staging.stg_sales_order_detail (sales_order_id, sales_order_detail_id, carrier_tracking_number, order_qty, product_id, special_offer_id, unit_price, unit_price_discount, line_total, modified_date)
VALUES (99902, 1, NULL, 4, 1, 1, 24.9950, 0.0000, 99.9800, '2014-11-01T00:00:00.000');

-- Remove PK to allow duplicate
ALTER TABLE staging.stg_sales_order_detail DROP CONSTRAINT pk_stg_sales_order_detail;

-- Insert duplicate detail row (same PK values, same data)
INSERT INTO staging.stg_sales_order_detail (sales_order_id, sales_order_detail_id, carrier_tracking_number, order_qty, product_id, special_offer_id, unit_price, unit_price_discount, line_total, modified_date)
VALUES (99902, 1, NULL, 4, 1, 1, 24.9950, 0.0000, 99.9800, '2014-11-01T00:00:00.000');

-- Restore PK (will fail if dupes still exist — that's intentional for this scenario)
-- The procedure should handle dedup before the PK is restored.
GO
