-- ==========================================================
-- Kimball DW Fixture — PostgreSQL Stored Procedures
-- Self-contained, idempotent: drops and recreates all procs
-- Run as: psql -U postgres -d kimball_fixture -f procedures/postgres.sql
-- Requires: schema/postgres.sql executed first
-- ==========================================================

-- ----------------------------------------------------------
-- 1. Drop all procedures (reverse dependency order)
-- ----------------------------------------------------------
DROP PROCEDURE IF EXISTS public.usp_exec_orchestrator_full_load;
DROP PROCEDURE IF EXISTS public.usp_validate_staging_counts;
DROP PROCEDURE IF EXISTS public.usp_load_gold_agg_batch;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_address_coverage;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_customer_segments;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_returns_analysis;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_channel_pivot;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_sales_by_category;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_employee_hierarchy;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_sales_by_territory;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_product_performance;
DROP PROCEDURE IF EXISTS public.usp_load_rpt_customer_lifetime_value;
DROP PROCEDURE IF EXISTS public.usp_load_fct_sales_by_channel;
DROP PROCEDURE IF EXISTS public.usp_load_fct_sales_summary;
DROP PROCEDURE IF EXISTS public.usp_load_fct_sales_historical;
DROP PROCEDURE IF EXISTS public.usp_load_fct_sales_daily(VARCHAR);
DROP PROCEDURE IF EXISTS public.usp_load_dim_address_and_credit_card;
DROP PROCEDURE IF EXISTS public.usp_load_dim_product_category;
DROP PROCEDURE IF EXISTS public.usp_load_dim_employee;
DROP PROCEDURE IF EXISTS public.usp_load_dim_product;
DROP PROCEDURE IF EXISTS public.usp_load_dim_customer;

-- ----------------------------------------------------------
-- 2. Drop + recreate complex views
-- ----------------------------------------------------------
DROP VIEW IF EXISTS staging.vw_enriched_sales CASCADE;
DROP VIEW IF EXISTS staging.vw_customer_360 CASCADE;

CREATE VIEW staging.vw_enriched_sales AS
WITH order_detail AS (
    SELECT
        d.sales_order_id,
        d.sales_order_detail_id,
        d.order_qty,
        d.product_id,
        d.unit_price,
        d.unit_price_discount,
        d.line_total,
        h.customer_id,
        h.order_date,
        h.territory_id,
        h.online_order_flag,
        CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
        ROW_NUMBER() OVER (PARTITION BY d.product_id ORDER BY h.order_date DESC) AS product_order_rank,
        LAG(d.line_total) OVER (PARTITION BY d.product_id ORDER BY h.order_date, d.sales_order_detail_id) AS prev_line_total,
        CASE WHEN r.return_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_returned
    FROM staging.stg_sales_order_detail d
    INNER JOIN staging.stg_sales_order_header h ON d.sales_order_id = h.sales_order_id
    LEFT JOIN staging.stg_returns r
        ON d.sales_order_id = r.sales_order_id
        AND d.sales_order_detail_id = r.sales_order_detail_id
)
SELECT
    od.*,
    CASE
        WHEN od.prev_line_total IS NULL THEN 'First'
        WHEN od.line_total > od.prev_line_total THEN 'Growth'
        WHEN od.line_total < od.prev_line_total THEN 'Decline'
        ELSE 'Stable'
    END AS sales_trend
FROM order_detail od;

CREATE VIEW staging.vw_customer_360 AS
WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.person_id,
        c.territory_id,
        CONCAT(p.first_name, ' ', COALESCE(p.middle_name || ' ', ''), p.last_name) AS full_name,
        COUNT(DISTINCT h.sales_order_id) AS total_orders,
        COALESCE(SUM(h.total_due), 0) AS total_revenue,
        AVG(h.total_due) AS avg_order_value,
        MIN(h.order_date) AS first_order_date,
        MAX(h.order_date) AS last_order_date
    FROM staging.stg_customer c
    LEFT JOIN staging.stg_person p ON c.person_id = p.business_entity_id
    LEFT JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
    GROUP BY c.customer_id, c.person_id, c.territory_id,
             p.first_name, p.middle_name, p.last_name
)
SELECT
    co.*,
    NTILE(4) OVER (ORDER BY co.total_revenue DESC) AS revenue_quartile,
    CASE
        WHEN co.total_orders = 0 THEN 'Inactive'
        WHEN co.total_revenue >= (SELECT AVG(total_due) * 3 FROM staging.stg_sales_order_header) THEN 'Platinum'
        WHEN co.total_revenue >= (SELECT AVG(total_due) FROM staging.stg_sales_order_header) THEN 'Gold'
        ELSE 'Silver'
    END AS customer_tier
FROM customer_orders co;

-- ==========================================================
-- 3. Dimension Load Procs (#1-#5)
-- ==========================================================

-- ----------------------------------------------------------
-- #1 usp_load_dim_customer
-- MERGE SCD2 from vw_stg_customer, EXEC call for address enrichment
-- Uses PG15+ MERGE
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_dim_customer()
LANGUAGE plpgsql AS $$
DECLARE
    v_now       TIMESTAMP := CURRENT_TIMESTAMP;
    v_updated   INTEGER := 0;
    v_inserted  INTEGER := 0;
BEGIN
    -- Expire existing rows where attributes changed
    MERGE INTO dim.dim_customer AS tgt
    USING (
        SELECT
            vc.customer_id,
            vc.person_id,
            vc.store_id,
            vc.territory_id,
            vc.full_name,
            vc.modified_date
        FROM staging.vw_stg_customer vc
    ) AS src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    WHEN MATCHED AND (
        COALESCE(tgt.full_name, '') <> COALESCE(src.full_name, '')
        OR COALESCE(tgt.territory_id, -1) <> COALESCE(src.territory_id, -1)
        OR COALESCE(tgt.store_id, -1) <> COALESCE(src.store_id, -1)
    ) THEN
        UPDATE SET
            valid_to = v_now,
            is_current = FALSE
    WHEN NOT MATCHED THEN
        INSERT (customer_id, person_id, store_id, full_name, territory_id, valid_from, valid_to, is_current)
        VALUES (src.customer_id, src.person_id, src.store_id, src.full_name, src.territory_id, v_now, NULL, TRUE);

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    -- Insert new current rows for expired records
    INSERT INTO dim.dim_customer (customer_id, person_id, store_id, full_name, territory_id, valid_from, valid_to, is_current)
    SELECT
        vc.customer_id,
        vc.person_id,
        vc.store_id,
        vc.full_name,
        vc.territory_id,
        v_now,
        NULL,
        TRUE
    FROM staging.vw_stg_customer vc
    WHERE EXISTS (
        SELECT 1 FROM dim.dim_customer d
        WHERE d.customer_id = vc.customer_id
          AND d.is_current = FALSE
          AND d.valid_to = v_now
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim.dim_customer d
        WHERE d.customer_id = vc.customer_id
          AND d.is_current = TRUE
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    -- Address enrichment via helper call
    CALL public.usp_load_dim_address_and_credit_card();

    RAISE NOTICE 'usp_load_dim_customer: updated=%, inserted=%', v_updated, v_inserted;
END;
$$;

-- ----------------------------------------------------------
-- #2 usp_load_dim_product
-- INSERT...ON CONFLICT SCD2 with multi-level CTE
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_dim_product()
LANGUAGE plpgsql AS $$
DECLARE
    v_now       TIMESTAMP := CURRENT_TIMESTAMP;
    v_inserted  INTEGER := 0;
BEGIN
    -- Expire changed rows
    UPDATE dim.dim_product tgt
    SET valid_to = v_now,
        is_current = FALSE
    FROM (
        WITH product_enriched AS (
            SELECT
                p.product_id,
                p.product_name,
                p.product_number,
                p.color,
                p.class,
                p.product_line,
                p.standard_cost,
                p.list_price,
                sc.subcategory_name AS product_subcategory,
                pc.category_name AS product_category,
                p.sell_start_date,
                p.sell_end_date
            FROM staging.stg_product p
            INNER JOIN staging.stg_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
            INNER JOIN staging.stg_product_category pc ON sc.product_category_id = pc.product_category_id
        )
        SELECT * FROM product_enriched
    ) src
    WHERE tgt.product_id = src.product_id
      AND tgt.is_current = TRUE
      AND (
          tgt.product_name <> src.product_name
          OR COALESCE(tgt.color, '') <> COALESCE(src.color, '')
          OR tgt.standard_cost <> src.standard_cost
          OR tgt.list_price <> src.list_price
          OR COALESCE(tgt.product_subcategory, '') <> COALESCE(src.product_subcategory, '')
          OR COALESCE(tgt.product_category, '') <> COALESCE(src.product_category, '')
      );

    -- Insert new current versions using ON CONFLICT for products already current
    WITH product_enriched AS (
        SELECT
            p.product_id,
            p.product_name,
            p.product_number,
            p.color,
            p.class,
            p.product_line,
            p.standard_cost,
            p.list_price,
            sc.subcategory_name AS product_subcategory,
            pc.category_name AS product_category,
            p.sell_start_date,
            p.sell_end_date
        FROM staging.stg_product p
        INNER JOIN staging.stg_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
        INNER JOIN staging.stg_product_category pc ON sc.product_category_id = pc.product_category_id
    )
    INSERT INTO dim.dim_product (product_id, product_name, product_number, color, class, product_line,
                                  standard_cost, list_price, product_subcategory, product_category,
                                  sell_start_date, sell_end_date, valid_from, valid_to, is_current)
    SELECT
        pe.product_id, pe.product_name, pe.product_number, pe.color, pe.class, pe.product_line,
        pe.standard_cost, pe.list_price, pe.product_subcategory, pe.product_category,
        pe.sell_start_date, pe.sell_end_date, v_now, NULL, TRUE
    FROM product_enriched pe
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.dim_product d
        WHERE d.product_id = pe.product_id AND d.is_current = TRUE
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    RAISE NOTICE 'usp_load_dim_product: inserted=%', v_inserted;
END;
$$;

-- ----------------------------------------------------------
-- #3 usp_load_dim_employee
-- MERGE SCD1 with self-join for manager_name, scalar subquery
-- Uses PG15+ MERGE
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_dim_employee()
LANGUAGE plpgsql AS $$
DECLARE
    v_now       TIMESTAMP := CURRENT_TIMESTAMP;
    v_count     INTEGER := 0;
BEGIN
    MERGE INTO dim.dim_employee AS tgt
    USING (
        SELECT
            e.business_entity_id AS employee_id,
            e.national_id_number,
            e.first_name,
            e.last_name,
            e.job_title,
            e.birth_date,
            e.gender,
            e.hire_date,
            e.salaried_flag,
            e.current_flag,
            COALESCE(m.first_name || ' ' || m.last_name, 'No Manager') AS manager_name,
            (SELECT COUNT(*) FROM staging.stg_employee sub
             WHERE sub.manager_id = e.business_entity_id) AS direct_reports_count
        FROM staging.stg_employee e
        LEFT JOIN staging.stg_employee m ON e.manager_id = m.business_entity_id
    ) AS src
    ON tgt.employee_id = src.employee_id AND tgt.is_current = TRUE
    WHEN MATCHED THEN
        UPDATE SET
            national_id_number = src.national_id_number,
            first_name         = src.first_name,
            last_name          = src.last_name,
            job_title          = src.job_title,
            birth_date         = src.birth_date,
            gender             = src.gender,
            hire_date          = src.hire_date,
            salaried_flag      = src.salaried_flag,
            current_flag       = src.current_flag,
            valid_from         = v_now
    WHEN NOT MATCHED THEN
        INSERT (employee_id, national_id_number, first_name, last_name, job_title,
                birth_date, gender, hire_date, salaried_flag, current_flag,
                valid_from, valid_to, is_current)
        VALUES (src.employee_id, src.national_id_number, src.first_name, src.last_name, src.job_title,
                src.birth_date, src.gender, src.hire_date, src.salaried_flag, src.current_flag,
                v_now, NULL, TRUE);

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_dim_employee: merged=% rows', v_count;
END;
$$;

-- ----------------------------------------------------------
-- #4 usp_load_dim_product_category
-- INSERT...ON CONFLICT SCD2 with EXISTS change detection
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_dim_product_category()
LANGUAGE plpgsql AS $$
DECLARE
    v_now       TIMESTAMP := CURRENT_TIMESTAMP;
    v_expired   INTEGER := 0;
    v_inserted  INTEGER := 0;
BEGIN
    -- Expire rows where category_name changed
    UPDATE dim.dim_product_category
    SET valid_to = v_now,
        is_current = FALSE
    WHERE is_current = TRUE
      AND EXISTS (
          SELECT 1
          FROM staging.stg_product_category src
          WHERE src.product_category_id = dim.dim_product_category.product_category_id
            AND src.category_name <> dim.dim_product_category.category_name
      );

    GET DIAGNOSTICS v_expired = ROW_COUNT;

    -- Insert new current rows (changed + brand new)
    INSERT INTO dim.dim_product_category (product_category_id, category_name, valid_from, valid_to, is_current)
    SELECT
        src.product_category_id,
        src.category_name,
        v_now,
        NULL,
        TRUE
    FROM staging.stg_product_category src
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.dim_product_category d
        WHERE d.product_category_id = src.product_category_id
          AND d.is_current = TRUE
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    RAISE NOTICE 'usp_load_dim_product_category: expired=%, inserted=%', v_expired, v_inserted;
END;
$$;

-- ----------------------------------------------------------
-- #5 usp_load_dim_address_and_credit_card
-- Multi-table: two MERGE statements, LEFT JOIN LATERAL
-- Uses PG15+ MERGE
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_dim_address_and_credit_card()
LANGUAGE plpgsql AS $$
DECLARE
    v_now             TIMESTAMP := CURRENT_TIMESTAMP;
    v_addr_count      INTEGER := 0;
    v_cc_count        INTEGER := 0;
BEGIN
    -- MERGE #1: dim_address SCD2
    MERGE INTO dim.dim_address AS tgt
    USING (
        SELECT
            a.address_id,
            a.address_line_1,
            a.city,
            a.state_province_id,
            a.postal_code
        FROM staging.stg_address a
    ) AS src
    ON tgt.address_id = src.address_id AND tgt.is_current = TRUE
    WHEN MATCHED AND (
        tgt.address_line_1 <> src.address_line_1
        OR tgt.city <> src.city
        OR tgt.postal_code <> src.postal_code
    ) THEN
        UPDATE SET
            valid_to = v_now,
            is_current = FALSE
    WHEN NOT MATCHED THEN
        INSERT (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
        VALUES (src.address_id, src.address_line_1, src.city, src.state_province_id, src.postal_code, v_now, NULL, TRUE);

    GET DIAGNOSTICS v_addr_count = ROW_COUNT;

    -- Insert new current rows for expired addresses
    INSERT INTO dim.dim_address (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
    SELECT
        a.address_id, a.address_line_1, a.city, a.state_province_id, a.postal_code,
        v_now, NULL, TRUE
    FROM staging.stg_address a
    WHERE EXISTS (
        SELECT 1 FROM dim.dim_address d
        WHERE d.address_id = a.address_id AND d.is_current = FALSE AND d.valid_to = v_now
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim.dim_address d
        WHERE d.address_id = a.address_id AND d.is_current = TRUE
    );

    -- MERGE #2: dim_credit_card SCD2 with LEFT JOIN LATERAL for latest card per address
    MERGE INTO dim.dim_credit_card AS tgt
    USING (
        SELECT
            cc.credit_card_id,
            cc.card_type,
            cc.exp_month,
            cc.exp_year
        FROM staging.stg_credit_card cc
        LEFT JOIN LATERAL (
            SELECT h.bill_to_address_id
            FROM staging.stg_sales_order_header h
            WHERE h.credit_card_id = cc.credit_card_id
            ORDER BY h.order_date DESC
            LIMIT 1
        ) AS latest_addr ON TRUE
    ) AS src
    ON tgt.credit_card_id = src.credit_card_id AND tgt.is_current = TRUE
    WHEN MATCHED AND (
        tgt.card_type <> src.card_type
        OR tgt.exp_month <> src.exp_month
        OR tgt.exp_year <> src.exp_year
    ) THEN
        UPDATE SET
            valid_to = v_now,
            is_current = FALSE
    WHEN NOT MATCHED THEN
        INSERT (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
        VALUES (src.credit_card_id, src.card_type, src.exp_month, src.exp_year, v_now, NULL, TRUE);

    GET DIAGNOSTICS v_cc_count = ROW_COUNT;

    -- Insert new current rows for expired credit cards
    INSERT INTO dim.dim_credit_card (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
    SELECT
        cc.credit_card_id, cc.card_type, cc.exp_month, cc.exp_year,
        v_now, NULL, TRUE
    FROM staging.stg_credit_card cc
    WHERE EXISTS (
        SELECT 1 FROM dim.dim_credit_card d
        WHERE d.credit_card_id = cc.credit_card_id AND d.is_current = FALSE AND d.valid_to = v_now
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim.dim_credit_card d
        WHERE d.credit_card_id = cc.credit_card_id AND d.is_current = TRUE
    );

    RAISE NOTICE 'usp_load_dim_address_and_credit_card: addr=%, cc=%', v_addr_count, v_cc_count;
END;
$$;

-- ==========================================================
-- 4. Fact Load Procs (#6-#9)
-- ==========================================================

-- ----------------------------------------------------------
-- #6 usp_load_fct_sales_daily
-- INSERT...SELECT with IF/ELSIF mode, CROSS JOIN LATERAL,
-- nested BEGIN/EXCEPTION error handling
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_fct_sales_daily(
    p_mode VARCHAR DEFAULT 'INCREMENTAL'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count     INTEGER := 0;
    v_avg_price     NUMERIC(19,4);
BEGIN
    -- Compute average price for discount validation
    SELECT AVG(unit_price) INTO v_avg_price
    FROM staging.stg_sales_order_detail;

    IF p_mode = 'FULL' THEN
        TRUNCATE TABLE fact.fct_sales;

        BEGIN
            INSERT INTO fact.fct_sales (
                sales_order_id, sales_order_detail_id, customer_key, product_key,
                date_key, address_key, credit_card_key, order_status_key,
                order_qty, unit_price, unit_price_discount, line_total
            )
            SELECT
                h.sales_order_id,
                d.sales_order_detail_id,
                dc.customer_key,
                dp.product_key,
                CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
                da.address_key,
                dcc.credit_card_key,
                dos.order_status_key,
                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total
            FROM staging.stg_sales_order_header h
            INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
            INNER JOIN dim.dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = TRUE
            INNER JOIN dim.dim_product dp ON d.product_id = dp.product_id AND dp.is_current = TRUE
            LEFT JOIN dim.dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = TRUE
            LEFT JOIN dim.dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = TRUE
            INNER JOIN dim.dim_order_status dos ON h.status = dos.order_status
            CROSS JOIN LATERAL (
                SELECT MAX(d2.line_total) AS max_line
                FROM staging.stg_sales_order_detail d2
                WHERE d2.sales_order_id = h.sales_order_id
            ) AS top_detail
            WHERE d.unit_price_discount <= (
                SELECT AVG(sd.unit_price_discount) + 0.5
                FROM staging.stg_sales_order_detail sd
                WHERE sd.sales_order_id = h.sales_order_id
            );

            GET DIAGNOSTICS v_row_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'usp_load_fct_sales_daily FULL mode failed: %', SQLERRM;
            RAISE;
        END;

    ELSIF p_mode = 'INCREMENTAL' THEN
        BEGIN
            INSERT INTO fact.fct_sales (
                sales_order_id, sales_order_detail_id, customer_key, product_key,
                date_key, address_key, credit_card_key, order_status_key,
                order_qty, unit_price, unit_price_discount, line_total
            )
            SELECT
                h.sales_order_id,
                d.sales_order_detail_id,
                dc.customer_key,
                dp.product_key,
                CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
                da.address_key,
                dcc.credit_card_key,
                dos.order_status_key,
                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total
            FROM staging.stg_sales_order_header h
            INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
            INNER JOIN dim.dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = TRUE
            INNER JOIN dim.dim_product dp ON d.product_id = dp.product_id AND dp.is_current = TRUE
            LEFT JOIN dim.dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = TRUE
            LEFT JOIN dim.dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = TRUE
            INNER JOIN dim.dim_order_status dos ON h.status = dos.order_status
            CROSS JOIN LATERAL (
                SELECT MAX(d2.line_total) AS max_line
                FROM staging.stg_sales_order_detail d2
                WHERE d2.sales_order_id = h.sales_order_id
            ) AS top_detail
            WHERE NOT EXISTS (
                SELECT 1 FROM fact.fct_sales f
                WHERE f.sales_order_id = h.sales_order_id
                  AND f.sales_order_detail_id = d.sales_order_detail_id
            )
            AND d.unit_price_discount <= (
                SELECT AVG(sd.unit_price_discount) + 0.5
                FROM staging.stg_sales_order_detail sd
                WHERE sd.sales_order_id = h.sales_order_id
            );

            GET DIAGNOSTICS v_row_count = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'usp_load_fct_sales_daily INCREMENTAL mode failed: %', SQLERRM;
            RAISE;
        END;
    END IF;

    RAISE NOTICE 'usp_load_fct_sales_daily: mode=%, rows=%', p_mode, v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #7 usp_load_fct_sales_historical
-- TRUNCATE + INSERT full rebuild, WHILE batch loop, ugly SQL
-- Nested control flow with BEGIN/EXCEPTION
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_fct_sales_historical()
LANGUAGE plpgsql AS $$
DECLARE
v_total INTEGER;
v_offset INTEGER := 0;
v_batch INTEGER := 1000;
v_threshold INTEGER := 500;
v_batch_rows INTEGER;
v_total_loaded INTEGER := 0;
BEGIN
TRUNCATE TABLE fact.fct_sales;
SELECT COUNT(*) INTO v_total FROM staging.stg_sales_order_detail;
IF v_total > v_threshold THEN
-- Batch load: ugly inline SQL, no CTEs, messy formatting
WHILE v_offset < v_total LOOP
BEGIN
INSERT INTO fact.fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key, date_key, address_key, credit_card_key, order_status_key, order_qty, unit_price, unit_price_discount, line_total)
SELECT soh.sales_order_id, sod.sales_order_detail_id, dc.customer_key, dp.product_key,
CAST(TO_CHAR(soh.order_date, 'YYYYMMDD') AS INTEGER),
da.address_key, dcc.credit_card_key, dos.order_status_key,
sod.order_qty, sod.unit_price, sod.unit_price_discount, sod.line_total
FROM (SELECT sd.sales_order_id, sd.sales_order_detail_id, sd.order_qty, sd.product_id, sd.unit_price, sd.unit_price_discount, sd.line_total FROM staging.stg_sales_order_detail sd ORDER BY sd.sales_order_id, sd.sales_order_detail_id LIMIT v_batch OFFSET v_offset) sod
INNER JOIN staging.stg_sales_order_header soh ON sod.sales_order_id = soh.sales_order_id
INNER JOIN dim.dim_customer dc ON soh.customer_id = dc.customer_id AND dc.is_current = TRUE
INNER JOIN dim.dim_product dp ON sod.product_id = dp.product_id AND dp.is_current = TRUE
LEFT JOIN dim.dim_address da ON soh.bill_to_address_id = da.address_id AND da.is_current = TRUE
LEFT JOIN dim.dim_credit_card dcc ON soh.credit_card_id = dcc.credit_card_id AND dcc.is_current = TRUE
INNER JOIN dim.dim_order_status dos ON soh.status = dos.order_status;
GET DIAGNOSTICS v_batch_rows = ROW_COUNT;
v_total_loaded := v_total_loaded + v_batch_rows;
EXCEPTION WHEN OTHERS THEN
RAISE WARNING 'usp_load_fct_sales_historical batch at offset % failed: %', v_offset, SQLERRM;
END;
v_offset := v_offset + v_batch;
END LOOP;
ELSE
-- Single-pass load for small datasets
BEGIN
INSERT INTO fact.fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key, date_key, address_key, credit_card_key, order_status_key, order_qty, unit_price, unit_price_discount, line_total)
SELECT soh.sales_order_id, sod.sales_order_detail_id, dc.customer_key, dp.product_key, CAST(TO_CHAR(soh.order_date, 'YYYYMMDD') AS INTEGER), da.address_key, dcc.credit_card_key, dos.order_status_key, sod.order_qty, sod.unit_price, sod.unit_price_discount, sod.line_total FROM staging.stg_sales_order_detail sod INNER JOIN staging.stg_sales_order_header soh ON sod.sales_order_id = soh.sales_order_id INNER JOIN dim.dim_customer dc ON soh.customer_id = dc.customer_id AND dc.is_current = TRUE INNER JOIN dim.dim_product dp ON sod.product_id = dp.product_id AND dp.is_current = TRUE LEFT JOIN dim.dim_address da ON soh.bill_to_address_id = da.address_id AND da.is_current = TRUE LEFT JOIN dim.dim_credit_card dcc ON soh.credit_card_id = dcc.credit_card_id AND dcc.is_current = TRUE INNER JOIN dim.dim_order_status dos ON soh.status = dos.order_status;
GET DIAGNOSTICS v_total_loaded = ROW_COUNT;
EXCEPTION WHEN OTHERS THEN
RAISE WARNING 'usp_load_fct_sales_historical single-pass failed: %', SQLERRM;
RAISE;
END;
END IF;
RAISE NOTICE 'usp_load_fct_sales_historical: loaded=% rows', v_total_loaded;
END;
$$;

-- ----------------------------------------------------------
-- #8 usp_load_fct_sales_summary
-- TRUNCATE + INSERT with ROLLUP and GROUPING()
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_fct_sales_summary()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE fact.fct_sales_summary;

    INSERT INTO fact.fct_sales_summary (date_key, product_key, total_qty, total_revenue, order_count)
    SELECT
        COALESCE(dd.date_key, 0)   AS date_key,
        COALESCE(fs.product_key, 0) AS product_key,
        SUM(fs.order_qty)           AS total_qty,
        SUM(fs.line_total)          AS total_revenue,
        COUNT(DISTINCT fs.sales_order_id) AS order_count
    FROM fact.fct_sales fs
    INNER JOIN dim.dim_date dd ON fs.date_key = dd.date_key
    GROUP BY ROLLUP (dd.year_number, dd.quarter_number, dd.month_number),
             fs.product_key,
             dd.date_key
    HAVING SUM(fs.line_total) > 0;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_fct_sales_summary: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #9 usp_load_fct_sales_by_channel
-- TRUNCATE + INSERT with CTE UNION ALL for channel split
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_fct_sales_by_channel()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE fact.fct_sales_by_channel;

    WITH channel_sales AS (
        -- Online orders
        SELECT
            CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
            'Online' AS channel,
            SUM(d.order_qty) AS total_qty,
            SUM(d.line_total) AS total_revenue,
            COUNT(DISTINCT h.sales_order_id) AS order_count
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = TRUE
        GROUP BY CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER)

        UNION ALL

        -- Store orders
        SELECT
            CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
            'Store' AS channel,
            SUM(d.order_qty) AS total_qty,
            SUM(d.line_total) AS total_revenue,
            COUNT(DISTINCT h.sales_order_id) AS order_count
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = FALSE
        GROUP BY CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER)
    )
    INSERT INTO fact.fct_sales_by_channel (date_key, channel, total_qty, total_revenue, order_count)
    SELECT date_key, channel, total_qty, total_revenue, order_count
    FROM channel_sales;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_fct_sales_by_channel: rows=%', v_row_count;
END;
$$;

-- ==========================================================
-- 5. Gold-Layer Procs (#10-#18)
-- ==========================================================

-- ----------------------------------------------------------
-- #10 usp_load_rpt_customer_lifetime_value
-- View-backed (vw_customer_360), RANK, NTILE, scalar subquery
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_customer_lifetime_value()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_customer_lifetime_value;

    INSERT INTO gold.rpt_customer_lifetime_value (
        customer_key, customer_id, full_name, total_orders, total_revenue,
        avg_order_value, first_order_date, last_order_date, customer_tier, revenue_quartile
    )
    SELECT
        dc.customer_key,
        cv.customer_id,
        cv.full_name,
        cv.total_orders,
        cv.total_revenue,
        (SELECT AVG(h.total_due)
         FROM staging.stg_sales_order_header h
         WHERE h.customer_id = cv.customer_id) AS avg_order_value,
        cv.first_order_date,
        cv.last_order_date,
        cv.customer_tier,
        cv.revenue_quartile
    FROM staging.vw_customer_360 cv
    INNER JOIN dim.dim_customer dc ON cv.customer_id = dc.customer_id AND dc.is_current = TRUE;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_customer_lifetime_value: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #11 usp_load_rpt_product_performance
-- View-backed (vw_enriched_sales), multi-level CTE with LAG, RANK
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_product_performance()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_product_performance;

    WITH monthly_agg AS (
        SELECT
            dp.product_key,
            dp.product_name,
            CAST(TO_CHAR(DATE_TRUNC('month', es.order_date), 'YYYYMMDD') AS INTEGER) AS date_key,
            SUM(es.line_total) AS monthly_revenue,
            SUM(es.order_qty) AS monthly_qty
        FROM staging.vw_enriched_sales es
        INNER JOIN dim.dim_product dp ON es.product_id = dp.product_id AND dp.is_current = TRUE
        GROUP BY dp.product_key, dp.product_name,
                 CAST(TO_CHAR(DATE_TRUNC('month', es.order_date), 'YYYYMMDD') AS INTEGER)
    ),
    with_lag AS (
        SELECT
            ma.product_key,
            ma.product_name,
            ma.date_key,
            ma.monthly_revenue,
            ma.monthly_qty,
            LAG(ma.monthly_revenue) OVER (PARTITION BY ma.product_key ORDER BY ma.date_key) AS prev_revenue,
            RANK() OVER (PARTITION BY ma.date_key ORDER BY ma.monthly_revenue DESC) AS revenue_rank
        FROM monthly_agg ma
    )
    INSERT INTO gold.rpt_product_performance (
        product_key, product_name, date_key, monthly_revenue, monthly_qty,
        revenue_rank, mom_growth_pct, trend
    )
    SELECT
        wl.product_key,
        wl.product_name,
        wl.date_key,
        wl.monthly_revenue,
        wl.monthly_qty,
        wl.revenue_rank,
        CASE
            WHEN wl.prev_revenue IS NOT NULL AND wl.prev_revenue > 0
            THEN ROUND(((wl.monthly_revenue - wl.prev_revenue) / wl.prev_revenue) * 100, 4)
            ELSE NULL
        END AS mom_growth_pct,
        CASE
            WHEN wl.prev_revenue IS NULL THEN 'First'
            WHEN wl.monthly_revenue > wl.prev_revenue THEN 'Growth'
            WHEN wl.monthly_revenue < wl.prev_revenue THEN 'Decline'
            ELSE 'Stable'
        END AS trend
    FROM with_lag wl;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_product_performance: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #12 usp_load_rpt_sales_by_territory
-- CROSS JOIN scaffold, LEFT JOIN actuals, RIGHT JOIN validation, RANK
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_sales_by_territory()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_sales_by_territory;

    INSERT INTO gold.rpt_sales_by_territory (
        territory_id, date_key, total_revenue, total_qty, order_count, territory_rank
    )
    WITH scaffold AS (
        SELECT t.territory_id, dk.date_key
        FROM (SELECT DISTINCT territory_id FROM staging.stg_sales_order_header WHERE territory_id IS NOT NULL) t
        CROSS JOIN (SELECT DISTINCT date_key FROM dim.dim_date) dk
    ),
    actuals AS (
        SELECT
            h.territory_id,
            CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER) AS date_key,
            SUM(d.line_total) AS total_revenue,
            SUM(d.order_qty)  AS total_qty,
            COUNT(DISTINCT h.sales_order_id) AS order_count
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.territory_id IS NOT NULL
        GROUP BY h.territory_id, CAST(TO_CHAR(h.order_date, 'YYYYMMDD') AS INTEGER)
    ),
    filled AS (
        SELECT
            s.territory_id,
            s.date_key,
            COALESCE(a.total_revenue, 0) AS total_revenue,
            COALESCE(a.total_qty, 0)     AS total_qty,
            COALESCE(a.order_count, 0)   AS order_count
        FROM scaffold s
        LEFT JOIN actuals a ON s.territory_id = a.territory_id AND s.date_key = a.date_key
    ),
    -- RIGHT JOIN validation: ensure all actuals have scaffolding
    validated AS (
        SELECT
            COALESCE(f.territory_id, a.territory_id) AS territory_id,
            COALESCE(f.date_key, a.date_key)         AS date_key,
            COALESCE(f.total_revenue, a.total_revenue, 0) AS total_revenue,
            COALESCE(f.total_qty, a.total_qty, 0)         AS total_qty,
            COALESCE(f.order_count, a.order_count, 0)     AS order_count
        FROM filled f
        RIGHT JOIN actuals a ON f.territory_id = a.territory_id AND f.date_key = a.date_key
    )
    SELECT
        v.territory_id,
        v.date_key,
        v.total_revenue,
        v.total_qty,
        v.order_count,
        RANK() OVER (PARTITION BY v.date_key ORDER BY v.total_revenue DESC) AS territory_rank
    FROM validated v
    WHERE v.total_revenue > 0;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_sales_by_territory: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #13 usp_load_rpt_employee_hierarchy
-- WITH RECURSIVE, scalar subquery for direct_reports_count
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_employee_hierarchy()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_employee_hierarchy;

    INSERT INTO gold.rpt_employee_hierarchy (
        employee_id, employee_name, manager_id, manager_name,
        hierarchy_level, manager_path, department, direct_reports_count
    )
    WITH RECURSIVE emp_hierarchy AS (
        -- Anchor: top-level managers (no manager)
        SELECT
            e.business_entity_id AS employee_id,
            e.first_name || ' ' || e.last_name AS employee_name,
            e.manager_id,
            CAST(NULL AS VARCHAR(101)) AS manager_name,
            1 AS hierarchy_level,
            CAST(e.first_name || ' ' || e.last_name AS VARCHAR(500)) AS manager_path,
            e.job_title AS department
        FROM staging.stg_employee e
        WHERE e.manager_id IS NULL

        UNION ALL

        -- Recursive: employees under a manager
        SELECT
            e.business_entity_id AS employee_id,
            e.first_name || ' ' || e.last_name AS employee_name,
            e.manager_id,
            eh.employee_name AS manager_name,
            eh.hierarchy_level + 1 AS hierarchy_level,
            CAST(eh.manager_path || ' > ' || e.first_name || ' ' || e.last_name AS VARCHAR(500)) AS manager_path,
            e.job_title AS department
        FROM staging.stg_employee e
        INNER JOIN emp_hierarchy eh ON e.manager_id = eh.employee_id
    )
    SELECT
        h.employee_id,
        h.employee_name,
        h.manager_id,
        h.manager_name,
        h.hierarchy_level,
        h.manager_path,
        h.department,
        (SELECT COUNT(*) FROM staging.stg_employee sub
         WHERE sub.manager_id = h.employee_id) AS direct_reports_count
    FROM emp_hierarchy h;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_employee_hierarchy: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #14 usp_load_rpt_sales_by_category
-- GROUPING SETS with GROUPING() function
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_sales_by_category()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_sales_by_category;

    INSERT INTO gold.rpt_sales_by_category (
        category_name, subcategory_name, date_key, total_revenue, total_qty, grouping_level
    )
    SELECT
        dp.product_category     AS category_name,
        dp.product_subcategory  AS subcategory_name,
        fs.date_key,
        SUM(fs.line_total)      AS total_revenue,
        SUM(fs.order_qty)       AS total_qty,
        GROUPING(dp.product_category) * 4
            + GROUPING(dp.product_subcategory) * 2
            + GROUPING(fs.date_key) AS grouping_level
    FROM fact.fct_sales fs
    INNER JOIN dim.dim_product dp ON fs.product_key = dp.product_key
    GROUP BY GROUPING SETS (
        (dp.product_category, dp.product_subcategory, fs.date_key),
        (dp.product_category, fs.date_key),
        (dp.product_category),
        ()
    );

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_sales_by_category: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #15 usp_load_rpt_channel_pivot
-- Conditional aggregation PIVOT + VALUES lateral UNPIVOT validation
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_channel_pivot()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count     INTEGER := 0;
    v_unpivot_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_channel_pivot;

    -- PIVOT via conditional aggregation (no native PIVOT in PG)
    INSERT INTO gold.rpt_channel_pivot (
        date_key, online_revenue, store_revenue, online_qty, store_qty,
        online_order_count, store_order_count
    )
    SELECT
        fsc.date_key,
        SUM(CASE WHEN fsc.channel = 'Online' THEN fsc.total_revenue ELSE 0 END) AS online_revenue,
        SUM(CASE WHEN fsc.channel = 'Store'  THEN fsc.total_revenue ELSE 0 END) AS store_revenue,
        SUM(CASE WHEN fsc.channel = 'Online' THEN fsc.total_qty ELSE 0 END) AS online_qty,
        SUM(CASE WHEN fsc.channel = 'Store'  THEN fsc.total_qty ELSE 0 END) AS store_qty,
        SUM(CASE WHEN fsc.channel = 'Online' THEN fsc.order_count ELSE 0 END) AS online_order_count,
        SUM(CASE WHEN fsc.channel = 'Store'  THEN fsc.order_count ELSE 0 END) AS store_order_count
    FROM fact.fct_sales_by_channel fsc
    GROUP BY fsc.date_key;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- UNPIVOT validation via VALUES lateral (PG pattern)
    SELECT COUNT(*) INTO v_unpivot_count
    FROM gold.rpt_channel_pivot cp
    CROSS JOIN LATERAL (
        VALUES
            ('Online', cp.online_revenue, cp.online_qty, cp.online_order_count),
            ('Store',  cp.store_revenue,  cp.store_qty,  cp.store_order_count)
    ) AS unpvt(channel, revenue, qty, order_count)
    WHERE unpvt.revenue > 0;

    RAISE NOTICE 'usp_load_rpt_channel_pivot: pivot_rows=%, unpivot_validation_rows=%', v_row_count, v_unpivot_count;
END;
$$;

-- ----------------------------------------------------------
-- #16 usp_load_rpt_returns_analysis
-- LEFT JOIN, NOT EXISTS, NOT IN with NULL guard, IN subquery, HAVING
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_returns_analysis()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_returns_analysis;

    INSERT INTO gold.rpt_returns_analysis (
        product_key, product_name, date_key, sales_qty, return_qty,
        return_rate, top_return_reason
    )
    SELECT
        dp.product_key,
        dp.product_name,
        fs.date_key,
        SUM(fs.order_qty) AS sales_qty,
        COALESCE(SUM(r.return_qty), 0) AS return_qty,
        CASE
            WHEN SUM(fs.order_qty) > 0
            THEN ROUND(COALESCE(SUM(r.return_qty), 0)::NUMERIC / SUM(fs.order_qty), 4)
            ELSE 0
        END AS return_rate,
        MAX(r.return_reason) AS top_return_reason
    FROM fact.fct_sales fs
    INNER JOIN dim.dim_product dp ON fs.product_key = dp.product_key
    LEFT JOIN staging.stg_returns r
        ON fs.sales_order_id = r.sales_order_id
        AND fs.sales_order_detail_id = r.sales_order_detail_id
    WHERE fs.sales_order_id IN (
        SELECT h.sales_order_id
        FROM staging.stg_sales_order_header h
        WHERE h.territory_id IS NOT NULL
    )
    AND fs.credit_card_key NOT IN (
        SELECT dcc.credit_card_key
        FROM dim.dim_credit_card dcc
        WHERE dcc.exp_year < 2010
        AND dcc.credit_card_key IS NOT NULL
    )
    GROUP BY dp.product_key, dp.product_name, fs.date_key
    HAVING SUM(fs.order_qty) >= 5;

    -- Identify never-returned products separately
    INSERT INTO gold.rpt_returns_analysis (
        product_key, product_name, date_key, sales_qty, return_qty,
        return_rate, top_return_reason
    )
    SELECT
        dp.product_key,
        dp.product_name,
        fs.date_key,
        SUM(fs.order_qty) AS sales_qty,
        0 AS return_qty,
        0 AS return_rate,
        NULL AS top_return_reason
    FROM fact.fct_sales fs
    INNER JOIN dim.dim_product dp ON fs.product_key = dp.product_key
    WHERE NOT EXISTS (
        SELECT 1 FROM staging.stg_returns r
        WHERE r.sales_order_id = fs.sales_order_id
          AND r.sales_order_detail_id = fs.sales_order_detail_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM gold.rpt_returns_analysis ra
        WHERE ra.product_key = dp.product_key AND ra.date_key = fs.date_key
    )
    GROUP BY dp.product_key, dp.product_name, fs.date_key
    HAVING SUM(fs.order_qty) >= 5;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_returns_analysis: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #17 usp_load_rpt_customer_segments
-- EXCEPT, INTERSECT, UNION for segment computation
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_customer_segments()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_customer_segments;

    INSERT INTO gold.rpt_customer_segments (customer_id, segment_name, total_revenue, total_orders)
    -- Active customers: all customers EXCEPT those with zero orders
    WITH all_customers AS (
        SELECT customer_id FROM staging.stg_customer
    ),
    inactive_customers AS (
        SELECT c.customer_id
        FROM staging.stg_customer c
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.stg_sales_order_header h
            WHERE h.customer_id = c.customer_id
        )
    ),
    active_set AS (
        SELECT customer_id FROM all_customers
        EXCEPT
        SELECT customer_id FROM inactive_customers
    ),
    high_value AS (
        SELECT h.customer_id
        FROM staging.stg_sales_order_header h
        GROUP BY h.customer_id
        HAVING SUM(h.total_due) >= (SELECT AVG(total_due) * 2 FROM staging.stg_sales_order_header)
    ),
    -- INTERSECT: active AND high-value
    active_high_value AS (
        SELECT customer_id FROM active_set
        INTERSECT
        SELECT customer_id FROM high_value
    ),
    at_risk AS (
        SELECT h.customer_id
        FROM staging.stg_sales_order_header h
        GROUP BY h.customer_id
        HAVING MAX(h.order_date) < CURRENT_TIMESTAMP - INTERVAL '365 days'
    ),
    -- UNION: combine all segments
    all_segments AS (
        SELECT ahv.customer_id, 'High-Value' AS segment_name
        FROM active_high_value ahv

        UNION

        SELECT ars.customer_id, 'At-Risk' AS segment_name
        FROM at_risk ars

        UNION

        SELECT act.customer_id, 'Active' AS segment_name
        FROM active_set act
        WHERE act.customer_id NOT IN (SELECT customer_id FROM active_high_value)
          AND act.customer_id NOT IN (SELECT customer_id FROM at_risk)
    )
    SELECT
        seg.customer_id,
        seg.segment_name,
        COALESCE(SUM(h.total_due), 0) AS total_revenue,
        COUNT(DISTINCT h.sales_order_id) AS total_orders
    FROM all_segments seg
    LEFT JOIN staging.stg_sales_order_header h ON seg.customer_id = h.customer_id
    GROUP BY seg.customer_id, seg.segment_name;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_customer_segments: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #18 usp_load_rpt_address_coverage
-- FULL OUTER JOIN, COALESCE, CASE gap classification
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_rpt_address_coverage()
LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER := 0;
BEGIN
    TRUNCATE TABLE gold.rpt_address_coverage;

    INSERT INTO gold.rpt_address_coverage (
        address_id, staging_city, dim_city, staging_postal_code, dim_postal_code, coverage_status
    )
    SELECT
        COALESCE(sa.address_id, da.address_id) AS address_id,
        sa.city           AS staging_city,
        da.city           AS dim_city,
        sa.postal_code    AS staging_postal_code,
        da.postal_code    AS dim_postal_code,
        CASE
            WHEN da.address_id IS NULL THEN 'new'
            WHEN sa.address_id IS NULL THEN 'orphan'
            ELSE 'matched'
        END AS coverage_status
    FROM staging.stg_address sa
    FULL OUTER JOIN dim.dim_address da
        ON sa.address_id = da.address_id AND da.is_current = TRUE;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    RAISE NOTICE 'usp_load_rpt_address_coverage: rows=%', v_row_count;
END;
$$;

-- ----------------------------------------------------------
-- #19 usp_load_gold_agg_batch
-- Multi-table via EXECUTE dynamic SQL: CUBE + ROLLUP
-- Writes to rpt_product_margin and rpt_date_sales_rollup
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_load_gold_agg_batch()
LANGUAGE plpgsql AS $$
DECLARE
    v_margin_count  INTEGER := 0;
    v_rollup_count  INTEGER := 0;
BEGIN
    -- Dynamic SQL #1: CUBE aggregation → rpt_product_margin
    EXECUTE '
        TRUNCATE TABLE gold.rpt_product_margin;

        INSERT INTO gold.rpt_product_margin (
            product_line, product_category, color,
            total_revenue, total_cost, total_margin, margin_pct, grouping_level
        )
        SELECT
            dp.product_line,
            dp.product_category,
            dp.color,
            SUM(fs.line_total) AS total_revenue,
            SUM(fs.order_qty * dp.standard_cost) AS total_cost,
            SUM(fs.line_total) - SUM(fs.order_qty * dp.standard_cost) AS total_margin,
            CASE
                WHEN SUM(fs.line_total) > 0
                THEN ROUND((SUM(fs.line_total) - SUM(fs.order_qty * dp.standard_cost)) / SUM(fs.line_total) * 100, 4)
                ELSE 0
            END AS margin_pct,
            GROUPING(dp.product_line) * 4
                + GROUPING(dp.product_category) * 2
                + GROUPING(dp.color) AS grouping_level
        FROM fact.fct_sales fs
        INNER JOIN dim.dim_product dp ON fs.product_key = dp.product_key
        GROUP BY CUBE (dp.product_line, dp.product_category, dp.color)
    ';

    GET DIAGNOSTICS v_margin_count = ROW_COUNT;

    -- Dynamic SQL #2: ROLLUP aggregation → rpt_date_sales_rollup
    EXECUTE '
        TRUNCATE TABLE gold.rpt_date_sales_rollup;

        INSERT INTO gold.rpt_date_sales_rollup (
            year_number, quarter_number, month_number,
            total_revenue, total_qty, order_count, rollup_level
        )
        SELECT
            dd.year_number,
            dd.quarter_number,
            dd.month_number,
            SUM(fs.line_total)  AS total_revenue,
            SUM(fs.order_qty)   AS total_qty,
            COUNT(DISTINCT fs.sales_order_id) AS order_count,
            GROUPING(dd.year_number) * 4
                + GROUPING(dd.quarter_number) * 2
                + GROUPING(dd.month_number) AS rollup_level
        FROM fact.fct_sales fs
        INNER JOIN dim.dim_date dd ON fs.date_key = dd.date_key
        GROUP BY ROLLUP (dd.year_number, dd.quarter_number, dd.month_number)
    ';

    GET DIAGNOSTICS v_rollup_count = ROW_COUNT;

    RAISE NOTICE 'usp_load_gold_agg_batch: margin_rows=%, rollup_rows=%', v_margin_count, v_rollup_count;
END;
$$;

-- ==========================================================
-- 6. Validation Helper + Exec Orchestrator (#20-#21)
-- ==========================================================

-- ----------------------------------------------------------
-- #20 usp_validate_staging_counts
-- Pre-flight validation: checks staging tables have data
-- Called by orchestrator before main load sequence
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_validate_staging_counts(
    p_raise_on_empty BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_customer_cnt   INTEGER;
    v_product_cnt    INTEGER;
    v_order_cnt      INTEGER;
    v_employee_cnt   INTEGER;
    v_has_gaps       BOOLEAN := FALSE;
BEGIN
    SELECT COUNT(*) INTO v_customer_cnt FROM staging.stg_customer;
    SELECT COUNT(*) INTO v_product_cnt  FROM staging.stg_product;
    SELECT COUNT(*) INTO v_order_cnt    FROM staging.stg_sales_order_header;
    SELECT COUNT(*) INTO v_employee_cnt FROM staging.stg_employee;

    IF v_customer_cnt = 0 THEN
        v_has_gaps := TRUE;
        RAISE WARNING 'staging.stg_customer is empty';
    END IF;

    IF v_product_cnt = 0 THEN
        v_has_gaps := TRUE;
        RAISE WARNING 'staging.stg_product is empty';
    END IF;

    IF v_order_cnt = 0 THEN
        v_has_gaps := TRUE;
        RAISE WARNING 'staging.stg_sales_order_header is empty';
    END IF;

    IF v_employee_cnt = 0 THEN
        v_has_gaps := TRUE;
        RAISE WARNING 'staging.stg_employee is empty';
    END IF;

    IF v_has_gaps AND p_raise_on_empty THEN
        RAISE EXCEPTION 'Staging validation failed: one or more tables are empty';
    END IF;

    RAISE NOTICE 'usp_validate_staging_counts: customers=%, products=%, orders=%, employees=%',
        v_customer_cnt, v_product_cnt, v_order_cnt, v_employee_cnt;
END;
$$;

-- ----------------------------------------------------------
-- #21 usp_exec_orchestrator_full_load
-- Calls procs in dependency order using CALL and EXECUTE
-- No direct table write — orchestration only
-- ----------------------------------------------------------
CREATE OR REPLACE PROCEDURE public.usp_exec_orchestrator_full_load()
LANGUAGE plpgsql AS $$
DECLARE
    v_step          INTEGER := 0;
    v_start_time    TIMESTAMP;
    v_end_time      TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;

    -- Step 0: pre-flight staging validation — direct CALL with param
    BEGIN
        CALL public.usp_validate_staging_counts(TRUE);
        RAISE NOTICE 'Orchestrator: staging validation passed';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator: staging validation failed: %', SQLERRM;
        RAISE;
    END;

    -- Step 1: dim_customer — direct CALL
    v_step := 1;
    BEGIN
        CALL public.usp_load_dim_customer();
        RAISE NOTICE 'Orchestrator step %: usp_load_dim_customer complete', v_step;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator step % failed: %', v_step, SQLERRM;
        RAISE;
    END;

    -- Step 2: dim_employee — CALL with note about SCD1 mode
    v_step := 2;
    BEGIN
        CALL public.usp_load_dim_employee();
        RAISE NOTICE 'Orchestrator step %: usp_load_dim_employee (SCD1) complete', v_step;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator step % failed: %', v_step, SQLERRM;
        RAISE;
    END;

    -- Step 3: fct_sales_daily — CALL with parameter (mode)
    v_step := 3;
    BEGIN
        CALL public.usp_load_fct_sales_daily('FULL');
        RAISE NOTICE 'Orchestrator step %: usp_load_fct_sales_daily complete', v_step;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator step % failed: %', v_step, SQLERRM;
        RAISE;
    END;

    -- Step 4: fct_sales_summary — direct CALL with return check
    v_step := 4;
    BEGIN
        CALL public.usp_load_fct_sales_summary();
        RAISE NOTICE 'Orchestrator step %: usp_load_fct_sales_summary complete', v_step;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator step % failed: %', v_step, SQLERRM;
        RAISE;
    END;

    -- Step 5: gold_agg_batch — dynamic EXECUTE string
    v_step := 5;
    BEGIN
        EXECUTE 'CALL public.usp_load_gold_agg_batch()';
        RAISE NOTICE 'Orchestrator step %: usp_load_gold_agg_batch complete', v_step;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Orchestrator step % failed: %', v_step, SQLERRM;
        RAISE;
    END;

    v_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'usp_exec_orchestrator_full_load: completed all % steps in %',
        v_step, v_end_time - v_start_time;
END;
$$;

-- ==========================================================
-- End of PostgreSQL stored procedures
-- ==========================================================
