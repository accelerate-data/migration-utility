-- Delta 05: Duplicate rows in staging (dedup in MERGE)
-- Two identical order detail rows with same PK values.
-- MERGE procedure must dedup before inserting into fct_sales.
--
-- Expected: only 1 fct_sales row for (sales_order_id=99902, detail_id=1)

INSERT INTO staging.stg_sales_order_header (sales_order_id, revision_number, order_date, due_date, ship_date, status, online_order_flag, sales_order_number, customer_id, sales_person_id, territory_id, bill_to_address_id, ship_to_address_id, ship_method_id, credit_card_id, sub_total, tax_amt, freight, total_due, modified_date)
VALUES (99902, 0, '2014-01-10 00:00:00', '2014-01-20 00:00:00', '2014-01-15 00:00:00', 5, TRUE, 'SO99902', 1, NULL, 1, 1, 1, 1, NULL, 99.9800, 8.0000, 2.5000, 110.4800, '2014-11-01 00:00:00');

-- Insert first detail row
INSERT INTO staging.stg_sales_order_detail (sales_order_id, sales_order_detail_id, carrier_tracking_number, order_qty, product_id, special_offer_id, unit_price, unit_price_discount, line_total, modified_date)
VALUES (99902, 1, NULL, 4, 1, 1, 24.9950, 0.0000, 99.9800, '2014-11-01 00:00:00');

-- Drop PK to allow duplicate
ALTER TABLE staging.stg_sales_order_detail DROP CONSTRAINT pk_stg_sales_order_detail;

-- Insert duplicate detail row
INSERT INTO staging.stg_sales_order_detail (sales_order_id, sales_order_detail_id, carrier_tracking_number, order_qty, product_id, special_offer_id, unit_price, unit_price_discount, line_total, modified_date)
VALUES (99902, 1, NULL, 4, 1, 1, 24.9950, 0.0000, 99.9800, '2014-11-01 00:00:00');
