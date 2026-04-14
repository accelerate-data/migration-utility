-- Delta 01: New customer and product never seen before (insert path)
-- Expected: new rows in stg_customer, stg_person, stg_product
-- Procedure should INSERT new dim rows (no existing match)

INSERT INTO staging.stg_person (business_entity_id, person_type, title, first_name, middle_name, last_name, suffix, email_promotion, modified_date)
VALUES (99901, 'IN', NULL, 'Delta', 'Test', 'CustomerOne', NULL, 0, '2014-07-01 00:00:00');

INSERT INTO staging.stg_customer (customer_id, person_id, store_id, territory_id, account_number, modified_date)
VALUES (99901, 99901, NULL, 1, 'AW99901', '2014-07-01 00:00:00');

INSERT INTO staging.stg_product (product_id, product_name, product_number, make_flag, finished_goods_flag, color, safety_stock_level, reorder_point, standard_cost, list_price, product_size, weight, days_to_manufacture, product_line, class, style, product_subcategory_id, product_model_id, sell_start_date, sell_end_date, discontinued_date, modified_date)
VALUES (99901, 'Delta Test Widget', 'DT-0001', TRUE, TRUE, 'Blue', 100, 50, 25.0000, 49.9900, 'M', 1.50, 1, 'R ', 'H ', 'U ', 1, NULL, '2014-07-01 00:00:00', NULL, NULL, '2014-07-01 00:00:00');
