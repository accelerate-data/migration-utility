-- Delta 01: New customer and product never seen before (insert path)
-- Expected: new rows in stg_customer, stg_person, stg_product
-- Procedure should INSERT new dim rows (no existing match)
USE KimballFixture;
GO

-- New person (business_entity_id = 99901, not in baseline)
INSERT INTO staging.stg_person (business_entity_id, person_type, title, first_name, middle_name, last_name, suffix, email_promotion, modified_date)
VALUES (99901, N'IN', NULL, N'Delta', N'Test', N'CustomerOne', NULL, 0, '2014-07-01T00:00:00.000');

-- New customer referencing the new person
INSERT INTO staging.stg_customer (customer_id, person_id, store_id, territory_id, account_number, modified_date)
VALUES (99901, 99901, NULL, 1, N'AW99901', '2014-07-01T00:00:00.000');

-- New product (product_id = 99901, not in baseline)
INSERT INTO staging.stg_product (product_id, product_name, product_number, make_flag, finished_goods_flag, color, safety_stock_level, reorder_point, standard_cost, list_price, product_size, weight, days_to_manufacture, product_line, class, style, product_subcategory_id, product_model_id, sell_start_date, sell_end_date, discontinued_date, modified_date)
VALUES (99901, N'Delta Test Widget', N'DT-0001', 1, 1, N'Blue', 100, 50, 25.0000, 49.9900, N'M', 1.50, 1, N'R ', N'H ', N'U ', 1, NULL, '2014-07-01T00:00:00.000', NULL, NULL, '2014-07-01T00:00:00.000');
GO
