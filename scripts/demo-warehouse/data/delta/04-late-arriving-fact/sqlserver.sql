-- Delta 04: Late-arriving fact (order for a historical dim member)
-- A sales order detail arrives referencing an old order date and existing dims.
-- Procedure should resolve dimension keys from historical dim rows.
--
-- Expected fct_sales assertions after procedure runs:
--   New fct_sales row with date_key from 2011, product_key and customer_key
--   resolved from existing dim rows
USE KimballFixture;
GO

-- Late-arriving order header (sales_order_id 99901, historical date)
INSERT INTO staging.stg_sales_order_header (sales_order_id, revision_number, order_date, due_date, ship_date, status, online_order_flag, sales_order_number, customer_id, sales_person_id, territory_id, bill_to_address_id, ship_to_address_id, ship_method_id, credit_card_id, sub_total, tax_amt, freight, total_due, modified_date)
VALUES (99901, 0, '2011-06-15T00:00:00.000', '2011-06-25T00:00:00.000', '2011-06-20T00:00:00.000', 5, 1, N'SO99901', 1, NULL, 1, 1, 1, 1, NULL, 49.9900, 4.0000, 1.2500, 55.2400, '2014-10-01T00:00:00.000');

-- Late-arriving order detail
INSERT INTO staging.stg_sales_order_detail (sales_order_id, sales_order_detail_id, carrier_tracking_number, order_qty, product_id, special_offer_id, unit_price, unit_price_discount, line_total, modified_date)
VALUES (99901, 1, NULL, 2, 1, 1, 24.9950, 0.0000, 49.9900, '2014-10-01T00:00:00.000');
GO
