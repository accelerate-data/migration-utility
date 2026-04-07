-- ==========================================================
-- Kimball DW Fixture — Oracle Stored Procedures
-- Self-contained, idempotent: drops and recreates all procs
-- Run as: kimball user in FREEPDB1
-- Requires: schema/oracle.sql installed first
-- ==========================================================

-- ----------------------------------------------------------
-- Drop all procedures (reverse dependency order)
-- ----------------------------------------------------------

BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_exec_orchestrator_full_load'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_gold_agg_batch'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_address_coverage'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_customer_segments'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_returns_analysis'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_channel_pivot'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_sales_by_category'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_employee_hierarchy'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_sales_by_territory'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_product_performance'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_rpt_customer_lifetime_value'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_fct_sales_by_channel'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_fct_sales_summary'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_fct_sales_historical'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_fct_sales_daily'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_dim_address_and_credit_card'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_dim_product_category'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_dim_employee'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_dim_product'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_load_dim_customer'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE usp_enrich_customer_address'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- ----------------------------------------------------------
-- Drop and recreate complex views
-- ----------------------------------------------------------

BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_enriched_sales'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP VIEW vw_customer_360'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

CREATE OR REPLACE VIEW vw_enriched_sales AS
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
        TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
        ROW_NUMBER() OVER (PARTITION BY d.product_id ORDER BY h.order_date DESC) AS product_order_rank,
        LAG(d.line_total) OVER (PARTITION BY d.product_id ORDER BY h.order_date, d.sales_order_detail_id) AS prev_line_total,
        CASE WHEN r.return_id IS NOT NULL THEN 1 ELSE 0 END AS is_returned
    FROM stg_sales_order_detail d
    INNER JOIN stg_sales_order_header h ON d.sales_order_id = h.sales_order_id
    LEFT JOIN stg_returns r
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
/

CREATE OR REPLACE VIEW vw_customer_360 AS
WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.person_id,
        c.territory_id,
        p.first_name || ' ' || COALESCE(p.middle_name || ' ', '') || p.last_name AS full_name,
        COUNT(DISTINCT h.sales_order_id) AS total_orders,
        SUM(h.total_due) AS total_revenue,
        AVG(h.total_due) AS avg_order_value,
        MIN(h.order_date) AS first_order_date,
        MAX(h.order_date) AS last_order_date
    FROM stg_customer c
    LEFT JOIN stg_person p ON c.person_id = p.business_entity_id
    LEFT JOIN stg_sales_order_header h ON c.customer_id = h.customer_id
    GROUP BY c.customer_id, c.person_id, c.territory_id,
             p.first_name, p.middle_name, p.last_name
)
SELECT
    co.*,
    NTILE(4) OVER (ORDER BY co.total_revenue DESC) AS revenue_quartile,
    CASE
        WHEN co.total_orders = 0 THEN 'Inactive'
        WHEN co.total_revenue >= (SELECT AVG(total_due) * 3 FROM stg_sales_order_header) THEN 'Platinum'
        WHEN co.total_revenue >= (SELECT AVG(total_due) FROM stg_sales_order_header) THEN 'Gold'
        ELSE 'Silver'
    END AS customer_tier
FROM customer_orders co;
/

-- ==========================================================
-- Helper procedure for address enrichment (called by #1)
-- ==========================================================

-- Helper: enrich customer address linkage
CREATE OR REPLACE PROCEDURE usp_enrich_customer_address
AS
    v_updated_count NUMBER := 0;
BEGIN
    UPDATE dim_customer dc
    SET dc.territory_id = (
        SELECT c.territory_id
        FROM stg_customer c
        WHERE c.customer_id = dc.customer_id
    )
    WHERE dc.is_current = 1
      AND dc.territory_id IS NULL;

    v_updated_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('usp_enrich_customer_address: updated ' || v_updated_count || ' rows');
END;
/

-- ==========================================================
-- #1: usp_load_dim_customer — SCD2 MERGE into dim_customer
-- View-backed, LEFT JOIN stg_person, CASE for full_name,
-- EXEC call to helper for address enrichment
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_dim_customer
AS
BEGIN
    -- SCD2 MERGE: expire existing rows where attributes changed
    MERGE INTO dim_customer tgt
    USING (
        SELECT
            vc.customer_id,
            vc.person_id,
            vc.full_name,
            NULL AS store_name,
            vc.territory_id,
            vc.modified_date
        FROM vw_stg_customer vc
    ) src
    ON (tgt.customer_id = src.customer_id AND tgt.is_current = 1)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.valid_to   = src.modified_date,
            tgt.is_current = 0
        WHERE NVL(tgt.full_name, '~') != NVL(src.full_name, '~')
           OR NVL(tgt.territory_id, -1) != NVL(src.territory_id, -1)
    WHEN NOT MATCHED THEN
        INSERT (customer_id, person_id, store_id, full_name, store_name, territory_id, valid_from, valid_to, is_current)
        VALUES (src.customer_id, src.person_id, NULL, src.full_name, src.store_name, src.territory_id, src.modified_date, NULL, 1);

    -- Insert new current rows for expired records
    INSERT INTO dim_customer (customer_id, person_id, store_id, full_name, store_name, territory_id, valid_from, valid_to, is_current)
    SELECT
        vc.customer_id,
        vc.person_id,
        NULL,
        vc.full_name,
        NULL,
        vc.territory_id,
        vc.modified_date,
        NULL,
        1
    FROM vw_stg_customer vc
    WHERE EXISTS (
        SELECT 1 FROM dim_customer dc
        WHERE dc.customer_id = vc.customer_id
          AND dc.is_current = 0
          AND dc.valid_to = vc.modified_date
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim_customer dc2
        WHERE dc2.customer_id = vc.customer_id
          AND dc2.is_current = 1
    );

    -- EXEC call to helper for address enrichment
    usp_enrich_customer_address;

    COMMIT;
END;
/

-- ==========================================================
-- #2: usp_load_dim_product — SCD2 MERGE with multi-level CTE
-- stg_product -> stg_product_subcategory -> stg_product_category
-- INNER JOINs across 3 staging tables
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_dim_product
AS
BEGIN
    MERGE INTO dim_product tgt
    USING (
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
                cat.category_name AS product_category,
                p.sell_start_date,
                p.sell_end_date,
                p.modified_date
            FROM stg_product p
            INNER JOIN stg_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
            INNER JOIN stg_product_category cat ON sc.product_category_id = cat.product_category_id
        )
        SELECT * FROM product_enriched
    ) src
    ON (tgt.product_id = src.product_id AND tgt.is_current = 1)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.valid_to   = src.modified_date,
            tgt.is_current = 0
        WHERE NVL(tgt.product_name, '~') != NVL(src.product_name, '~')
           OR NVL(tgt.standard_cost, -1) != NVL(src.standard_cost, -1)
           OR NVL(tgt.list_price, -1) != NVL(src.list_price, -1)
           OR NVL(tgt.product_subcategory, '~') != NVL(src.product_subcategory, '~')
           OR NVL(tgt.product_category, '~') != NVL(src.product_category, '~')
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, product_number, color, class, product_line,
                standard_cost, list_price, product_subcategory, product_category,
                sell_start_date, sell_end_date, valid_from, valid_to, is_current)
        VALUES (src.product_id, src.product_name, src.product_number, src.color, src.class, src.product_line,
                src.standard_cost, src.list_price, src.product_subcategory, src.product_category,
                src.sell_start_date, src.sell_end_date, src.modified_date, NULL, 1);

    -- Insert new current rows for expired records
    INSERT INTO dim_product (product_id, product_name, product_number, color, class, product_line,
                             standard_cost, list_price, product_subcategory, product_category,
                             sell_start_date, sell_end_date, valid_from, valid_to, is_current)
    SELECT
        pe.product_id, pe.product_name, pe.product_number, pe.color, pe.class, pe.product_line,
        pe.standard_cost, pe.list_price, pe.product_subcategory, pe.product_category,
        pe.sell_start_date, pe.sell_end_date, pe.modified_date, NULL, 1
    FROM (
        SELECT
            p.product_id, p.product_name, p.product_number, p.color, p.class, p.product_line,
            p.standard_cost, p.list_price,
            sc.subcategory_name AS product_subcategory,
            cat.category_name AS product_category,
            p.sell_start_date, p.sell_end_date, p.modified_date
        FROM stg_product p
        INNER JOIN stg_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
        INNER JOIN stg_product_category cat ON sc.product_category_id = cat.product_category_id
    ) pe
    WHERE EXISTS (
        SELECT 1 FROM dim_product dp
        WHERE dp.product_id = pe.product_id
          AND dp.is_current = 0
          AND dp.valid_to = pe.modified_date
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim_product dp2
        WHERE dp2.product_id = pe.product_id
          AND dp2.is_current = 1
    );

    COMMIT;
END;
/

-- ==========================================================
-- #3: usp_load_dim_employee — SCD1 MERGE
-- Self-join on manager_id for manager_name
-- COALESCE for nullable manager, scalar subquery for direct_reports_count
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_dim_employee
AS
BEGIN
    MERGE INTO dim_employee tgt
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
            e.modified_date
        FROM stg_employee e
    ) src
    ON (tgt.employee_id = src.employee_id)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.national_id_number = src.national_id_number,
            tgt.first_name         = src.first_name,
            tgt.last_name          = src.last_name,
            tgt.job_title          = src.job_title,
            tgt.birth_date         = src.birth_date,
            tgt.gender             = src.gender,
            tgt.hire_date          = src.hire_date,
            tgt.salaried_flag      = src.salaried_flag,
            tgt.current_flag       = src.current_flag,
            tgt.valid_from         = src.modified_date,
            tgt.is_current         = 1
    WHEN NOT MATCHED THEN
        INSERT (employee_id, national_id_number, first_name, last_name, job_title,
                birth_date, gender, hire_date, salaried_flag, current_flag,
                valid_from, valid_to, is_current)
        VALUES (src.employee_id, src.national_id_number, src.first_name, src.last_name, src.job_title,
                src.birth_date, src.gender, src.hire_date, src.salaried_flag, src.current_flag,
                src.modified_date, NULL, 1);

    COMMIT;
END;
/

-- ==========================================================
-- #4: usp_load_dim_product_category — SCD2 MERGE
-- EXISTS check for change detection
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_dim_product_category
AS
BEGIN
    -- Expire changed rows
    UPDATE dim_product_category dpc
    SET dpc.valid_to   = (SELECT pc.modified_date FROM stg_product_category pc WHERE pc.product_category_id = dpc.product_category_id),
        dpc.is_current = 0
    WHERE dpc.is_current = 1
      AND EXISTS (
          SELECT 1 FROM stg_product_category pc
          WHERE pc.product_category_id = dpc.product_category_id
            AND pc.category_name != dpc.category_name
      );

    -- Insert new current version for changed or new rows
    MERGE INTO dim_product_category tgt
    USING (
        SELECT
            pc.product_category_id,
            pc.category_name,
            pc.modified_date
        FROM stg_product_category pc
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_product_category dpc
            WHERE dpc.product_category_id = pc.product_category_id
              AND dpc.is_current = 1
        )
    ) src
    ON (1 = 0)
    WHEN NOT MATCHED THEN
        INSERT (product_category_id, category_name, valid_from, valid_to, is_current)
        VALUES (src.product_category_id, src.category_name, src.modified_date, NULL, 1);

    COMMIT;
END;
/

-- ==========================================================
-- #5: usp_load_dim_address_and_credit_card — Multi-table MERGE
-- Two MERGE statements: dim_address + dim_credit_card
-- LEFT JOIN LATERAL for latest credit card per address
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_dim_address_and_credit_card
AS
BEGIN
    -- MERGE #1: SCD2 on dim_address from stg_address
    MERGE INTO dim_address tgt
    USING (
        SELECT
            a.address_id,
            a.address_line_1,
            a.city,
            a.state_province_id,
            a.postal_code,
            a.modified_date
        FROM stg_address a
    ) src
    ON (tgt.address_id = src.address_id AND tgt.is_current = 1)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.valid_to   = src.modified_date,
            tgt.is_current = 0
        WHERE NVL(tgt.address_line_1, '~') != NVL(src.address_line_1, '~')
           OR NVL(tgt.city, '~') != NVL(src.city, '~')
           OR NVL(tgt.postal_code, '~') != NVL(src.postal_code, '~')
    WHEN NOT MATCHED THEN
        INSERT (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
        VALUES (src.address_id, src.address_line_1, src.city, src.state_province_id, src.postal_code, src.modified_date, NULL, 1);

    -- Insert new current rows for expired address records
    INSERT INTO dim_address (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
    SELECT
        a.address_id, a.address_line_1, a.city, a.state_province_id, a.postal_code, a.modified_date, NULL, 1
    FROM stg_address a
    WHERE EXISTS (
        SELECT 1 FROM dim_address da
        WHERE da.address_id = a.address_id AND da.is_current = 0 AND da.valid_to = a.modified_date
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim_address da2
        WHERE da2.address_id = a.address_id AND da2.is_current = 1
    );

    -- MERGE #2: SCD2 on dim_credit_card from stg_credit_card
    -- LEFT JOIN LATERAL to get latest credit card per address (cross-entity correlation)
    MERGE INTO dim_credit_card tgt
    USING (
        SELECT
            cc.credit_card_id,
            cc.card_type,
            cc.exp_month,
            cc.exp_year,
            cc.modified_date
        FROM stg_credit_card cc
        LEFT JOIN LATERAL (
            SELECT h.bill_to_address_id
            FROM stg_sales_order_header h
            WHERE h.credit_card_id = cc.credit_card_id
            ORDER BY h.order_date DESC
            FETCH FIRST 1 ROW ONLY
        ) latest_addr ON 1 = 1
    ) src
    ON (tgt.credit_card_id = src.credit_card_id AND tgt.is_current = 1)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.valid_to   = src.modified_date,
            tgt.is_current = 0
        WHERE NVL(tgt.card_type, '~') != NVL(src.card_type, '~')
           OR NVL(tgt.exp_month, -1) != NVL(src.exp_month, -1)
           OR NVL(tgt.exp_year, -1) != NVL(src.exp_year, -1)
    WHEN NOT MATCHED THEN
        INSERT (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
        VALUES (src.credit_card_id, src.card_type, src.exp_month, src.exp_year, src.modified_date, NULL, 1);

    -- Insert new current rows for expired credit card records
    INSERT INTO dim_credit_card (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
    SELECT
        cc.credit_card_id, cc.card_type, cc.exp_month, cc.exp_year, cc.modified_date, NULL, 1
    FROM stg_credit_card cc
    WHERE EXISTS (
        SELECT 1 FROM dim_credit_card dcc
        WHERE dcc.credit_card_id = cc.credit_card_id AND dcc.is_current = 0 AND dcc.valid_to = cc.modified_date
    )
    AND NOT EXISTS (
        SELECT 1 FROM dim_credit_card dcc2
        WHERE dcc2.credit_card_id = cc.credit_card_id AND dcc2.is_current = 1
    );

    COMMIT;
END;
/

-- ==========================================================
-- #6: usp_load_fct_sales_daily — Incremental fact load
-- INSERT...SELECT with multi-table JOIN, EXISTS filter,
-- IF/ELSIF for mode, EXCEPTION for error handling,
-- CROSS JOIN LATERAL for top-N enrichment
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_fct_sales_daily(
    p_mode IN VARCHAR2 DEFAULT 'INCREMENTAL'
)
AS
    v_row_count NUMBER := 0;
    v_avg_price NUMBER(19,4);
BEGIN
    -- Derived table for average price calculation
    SELECT AVG(unit_price) INTO v_avg_price FROM stg_sales_order_detail;

    IF p_mode = 'FULL' THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE fct_sales';

        INSERT INTO fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key,
                               date_key, address_key, credit_card_key, order_status_key,
                               order_qty, unit_price, unit_price_discount, line_total)
        SELECT
            h.sales_order_id,
            d.sales_order_detail_id,
            dc.customer_key,
            dp.product_key,
            TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
            da.address_key,
            dcc.credit_card_key,
            dos.order_status_key,
            d.order_qty,
            d.unit_price,
            d.unit_price_discount,
            d.line_total
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        INNER JOIN dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = 1
        INNER JOIN dim_product dp ON d.product_id = dp.product_id AND dp.is_current = 1
        LEFT JOIN dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = 1
        LEFT JOIN dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
        INNER JOIN dim_order_status dos ON h.status = dos.order_status
        -- CROSS JOIN LATERAL for top-N order detail enrichment
        CROSS JOIN LATERAL (
            SELECT MAX(d2.unit_price) AS max_detail_price
            FROM stg_sales_order_detail d2
            WHERE d2.sales_order_id = h.sales_order_id
        ) top_detail
        -- Correlated subquery for discount validation
        WHERE d.unit_price_discount <= (
            SELECT MAX(d3.unit_price_discount)
            FROM stg_sales_order_detail d3
            WHERE d3.sales_order_id = h.sales_order_id
        );

        v_row_count := SQL%ROWCOUNT;

    ELSIF p_mode = 'INCREMENTAL' THEN
        INSERT INTO fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key,
                               date_key, address_key, credit_card_key, order_status_key,
                               order_qty, unit_price, unit_price_discount, line_total)
        SELECT
            h.sales_order_id,
            d.sales_order_detail_id,
            dc.customer_key,
            dp.product_key,
            TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
            da.address_key,
            dcc.credit_card_key,
            dos.order_status_key,
            d.order_qty,
            d.unit_price,
            d.unit_price_discount,
            d.line_total
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        INNER JOIN dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = 1
        INNER JOIN dim_product dp ON d.product_id = dp.product_id AND dp.is_current = 1
        LEFT JOIN dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = 1
        LEFT JOIN dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
        INNER JOIN dim_order_status dos ON h.status = dos.order_status
        CROSS JOIN LATERAL (
            SELECT MAX(d2.unit_price) AS max_detail_price
            FROM stg_sales_order_detail d2
            WHERE d2.sales_order_id = h.sales_order_id
        ) top_detail
        WHERE NOT EXISTS (
            SELECT 1 FROM fct_sales fs
            WHERE fs.sales_order_id = h.sales_order_id
              AND fs.sales_order_detail_id = d.sales_order_detail_id
        )
        AND d.unit_price_discount <= (
            SELECT MAX(d3.unit_price_discount)
            FROM stg_sales_order_detail d3
            WHERE d3.sales_order_id = h.sales_order_id
        );

        v_row_count := SQL%ROWCOUNT;
    END IF;

    DBMS_OUTPUT.PUT_LINE('usp_load_fct_sales_daily: mode=' || p_mode || ' rows=' || v_row_count);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('usp_load_fct_sales_daily ERROR: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- ==========================================================
-- #7: usp_load_fct_sales_historical — Full rebuild (CONFLICT with #6)
-- TRUNCATE + INSERT, intentionally ugly SQL, no CTEs
-- WHILE batch loop (chunks of 1000), nested control flow
-- IF inside WHILE inside EXCEPTION handler
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_fct_sales_historical
AS
    v_offset     NUMBER := 0;
    v_batch_size NUMBER := 1000;
    v_total      NUMBER;
    v_batch_rows NUMBER;
    v_threshold  NUMBER := 500;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE fct_sales';

    SELECT COUNT(*) INTO v_total FROM stg_sales_order_detail;

    IF v_total > v_threshold THEN
        -- Batch loop: intentionally ugly, all inline joins, messy formatting
        WHILE v_offset < v_total LOOP
            INSERT INTO fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key,
date_key, address_key, credit_card_key, order_status_key,
order_qty, unit_price, unit_price_discount, line_total)
SELECT * FROM (
SELECT sq.sales_order_id, sq.sales_order_detail_id, sq.customer_key, sq.product_key,
sq.date_key, sq.address_key, sq.credit_card_key, sq.order_status_key,
sq.order_qty, sq.unit_price, sq.unit_price_discount, sq.line_total
FROM (
SELECT h.sales_order_id, d.sales_order_detail_id,
dc.customer_key, dp.product_key,
TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
da.address_key, dcc.credit_card_key, dos.order_status_key,
d.order_qty, d.unit_price, d.unit_price_discount, d.line_total,
ROW_NUMBER() OVER (ORDER BY h.sales_order_id, d.sales_order_detail_id) AS rn
FROM stg_sales_order_header h
INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
INNER JOIN dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = 1
INNER JOIN dim_product dp ON d.product_id = dp.product_id AND dp.is_current = 1
LEFT JOIN dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = 1
LEFT JOIN dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
INNER JOIN dim_order_status dos ON h.status = dos.order_status
) sq
WHERE sq.rn > v_offset AND sq.rn <= v_offset + v_batch_size
);

            v_batch_rows := SQL%ROWCOUNT;
            v_offset := v_offset + v_batch_size;

            IF v_batch_rows = 0 THEN
                EXIT;
            END IF;

            COMMIT;
        END LOOP;
    ELSE
        -- Single-pass for small datasets
        INSERT INTO fct_sales (sales_order_id, sales_order_detail_id, customer_key, product_key,
                               date_key, address_key, credit_card_key, order_status_key,
                               order_qty, unit_price, unit_price_discount, line_total)
        SELECT h.sales_order_id, d.sales_order_detail_id, dc.customer_key, dp.product_key,
               TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')), da.address_key, dcc.credit_card_key,
               dos.order_status_key, d.order_qty, d.unit_price, d.unit_price_discount, d.line_total
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        INNER JOIN dim_customer dc ON h.customer_id = dc.customer_id AND dc.is_current = 1
        INNER JOIN dim_product dp ON d.product_id = dp.product_id AND dp.is_current = 1
        LEFT JOIN dim_address da ON h.bill_to_address_id = da.address_id AND da.is_current = 1
        LEFT JOIN dim_credit_card dcc ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
        INNER JOIN dim_order_status dos ON h.status = dos.order_status;

        COMMIT;
    END IF;

    DBMS_OUTPUT.PUT_LINE('usp_load_fct_sales_historical: loaded ' || v_total || ' total rows');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('usp_load_fct_sales_historical ERROR: ' || SQLERRM);
        ROLLBACK;
        RAISE;
END;
/

-- ==========================================================
-- #8: usp_load_fct_sales_summary — TRUNCATE + INSERT
-- GROUP BY with ROLLUP (year, quarter, month) + GROUPING()
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_fct_sales_summary
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE fct_sales_summary';

    INSERT INTO fct_sales_summary (date_key, product_key, total_qty, total_revenue, order_count)
    SELECT
        fs.date_key,
        fs.product_key,
        SUM(fs.order_qty)   AS total_qty,
        SUM(fs.line_total)  AS total_revenue,
        COUNT(*)            AS order_count
    FROM fct_sales fs
    INNER JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY ROLLUP(dd.year_number, dd.quarter_number, dd.month_number), fs.date_key, fs.product_key
    HAVING GROUPING(dd.year_number) = 0;

    COMMIT;
END;
/

-- ==========================================================
-- #9: usp_load_fct_sales_by_channel — TRUNCATE + INSERT
-- CTE with UNION ALL: online vs store orders
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_fct_sales_by_channel
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE fct_sales_by_channel';

    INSERT INTO fct_sales_by_channel (date_key, channel, total_qty, total_revenue, order_count)
    WITH channel_split AS (
        SELECT
            TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
            'Online' AS channel,
            d.order_qty,
            d.line_total,
            h.sales_order_id
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = 1

        UNION ALL

        SELECT
            TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
            'Store' AS channel,
            d.order_qty,
            d.line_total,
            h.sales_order_id
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = 0
    )
    SELECT
        date_key,
        channel,
        SUM(order_qty)                   AS total_qty,
        SUM(line_total)                  AS total_revenue,
        COUNT(DISTINCT sales_order_id)   AS order_count
    FROM channel_split
    GROUP BY date_key, channel;

    COMMIT;
END;
/

-- ==========================================================
-- #10: usp_load_rpt_customer_lifetime_value — TRUNCATE + INSERT
-- View-backed from vw_customer_360
-- Window functions: RANK, NTILE(4), scalar subquery for avg order value
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_customer_lifetime_value
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_customer_lifetime_value';

    INSERT INTO rpt_customer_lifetime_value (customer_key, customer_id, full_name, total_orders,
                                             total_revenue, avg_order_value, first_order_date,
                                             last_order_date, customer_tier, revenue_quartile)
    SELECT
        dc.customer_key,
        v.customer_id,
        v.full_name,
        NVL(v.total_orders, 0),
        NVL(v.total_revenue, 0),
        v.avg_order_value,
        v.first_order_date,
        v.last_order_date,
        v.customer_tier,
        v.revenue_quartile
    FROM vw_customer_360 v
    INNER JOIN dim_customer dc ON v.customer_id = dc.customer_id AND dc.is_current = 1;

    COMMIT;
END;
/

-- ==========================================================
-- #11: usp_load_rpt_product_performance — TRUNCATE + INSERT
-- View-backed from vw_enriched_sales
-- Multi-level CTE: monthly aggregate -> LAG for MoM growth -> RANK
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_product_performance
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_product_performance';

    INSERT INTO rpt_product_performance (product_key, product_name, date_key,
                                         monthly_revenue, monthly_qty, revenue_rank,
                                         mom_growth_pct, trend)
    WITH monthly_agg AS (
        SELECT
            dp.product_key,
            dp.product_name,
            es.date_key,
            SUM(es.line_total)  AS monthly_revenue,
            SUM(es.order_qty)   AS monthly_qty
        FROM vw_enriched_sales es
        INNER JOIN dim_product dp ON es.product_id = dp.product_id AND dp.is_current = 1
        GROUP BY dp.product_key, dp.product_name, es.date_key
    ),
    with_growth AS (
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
    SELECT
        wg.product_key,
        wg.product_name,
        wg.date_key,
        wg.monthly_revenue,
        wg.monthly_qty,
        wg.revenue_rank,
        CASE
            WHEN wg.prev_revenue IS NULL OR wg.prev_revenue = 0 THEN NULL
            ELSE ROUND((wg.monthly_revenue - wg.prev_revenue) / wg.prev_revenue * 100, 4)
        END AS mom_growth_pct,
        CASE
            WHEN wg.prev_revenue IS NULL THEN 'First'
            WHEN wg.monthly_revenue > wg.prev_revenue THEN 'Growth'
            WHEN wg.monthly_revenue < wg.prev_revenue THEN 'Decline'
            ELSE 'Stable'
        END AS trend
    FROM with_growth wg;

    COMMIT;
END;
/

-- ==========================================================
-- #12: usp_load_rpt_sales_by_territory — TRUNCATE + INSERT
-- CROSS JOIN scaffold, LEFT JOIN actuals, RIGHT JOIN validation, RANK
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_sales_by_territory
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_sales_by_territory';

    INSERT INTO rpt_sales_by_territory (territory_id, date_key, total_revenue, total_qty, order_count, territory_rank)
    WITH territory_dates AS (
        -- CROSS JOIN scaffold: all territory x date combos
        SELECT
            t.territory_id,
            d.date_key
        FROM (SELECT DISTINCT territory_id FROM stg_sales_order_header WHERE territory_id IS NOT NULL) t
        CROSS JOIN (SELECT DISTINCT date_key FROM dim_date) d
    ),
    actual_sales AS (
        SELECT
            h.territory_id,
            TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD')) AS date_key,
            SUM(det.line_total)                   AS total_revenue,
            SUM(det.order_qty)                    AS total_qty,
            COUNT(DISTINCT h.sales_order_id)      AS order_count
        FROM stg_sales_order_header h
        INNER JOIN stg_sales_order_detail det ON h.sales_order_id = det.sales_order_id
        WHERE h.territory_id IS NOT NULL
        GROUP BY h.territory_id, TO_NUMBER(TO_CHAR(h.order_date, 'YYYYMMDD'))
    ),
    scaffold_filled AS (
        -- LEFT JOIN to zero-fill gaps
        SELECT
            td.territory_id,
            td.date_key,
            NVL(a.total_revenue, 0) AS total_revenue,
            NVL(a.total_qty, 0)     AS total_qty,
            NVL(a.order_count, 0)   AS order_count
        FROM territory_dates td
        LEFT JOIN actual_sales a ON td.territory_id = a.territory_id AND td.date_key = a.date_key
    ),
    -- RIGHT JOIN variant for territory coverage validation
    validated AS (
        SELECT
            sf.territory_id,
            sf.date_key,
            sf.total_revenue,
            sf.total_qty,
            sf.order_count
        FROM actual_sales act
        RIGHT JOIN scaffold_filled sf ON act.territory_id = sf.territory_id AND act.date_key = sf.date_key
    )
    SELECT
        v.territory_id,
        v.date_key,
        v.total_revenue,
        v.total_qty,
        v.order_count,
        RANK() OVER (PARTITION BY v.date_key ORDER BY v.total_revenue DESC) AS territory_rank
    FROM validated v;

    COMMIT;
END;
/

-- ==========================================================
-- #13: usp_load_rpt_employee_hierarchy — TRUNCATE + INSERT
-- Recursive CTE (Oracle 12c+), manager_path string agg,
-- Scalar subquery for direct_reports_count
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_employee_hierarchy
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_employee_hierarchy';

    INSERT INTO rpt_employee_hierarchy (employee_id, employee_name, manager_id, manager_name,
                                        hierarchy_level, manager_path, department, direct_reports_count)
    WITH emp_hierarchy (employee_id, first_name, last_name, manager_id, hierarchy_level, manager_path) AS (
        -- Anchor: top-level employees (no manager)
        SELECT
            e.business_entity_id AS employee_id,
            e.first_name,
            e.last_name,
            e.manager_id,
            1 AS hierarchy_level,
            e.first_name || ' ' || e.last_name AS manager_path
        FROM stg_employee e
        WHERE e.manager_id IS NULL

        UNION ALL

        -- Recursive: employees with managers
        SELECT
            e.business_entity_id AS employee_id,
            e.first_name,
            e.last_name,
            e.manager_id,
            eh.hierarchy_level + 1,
            eh.manager_path || ' > ' || e.first_name || ' ' || e.last_name
        FROM stg_employee e
        INNER JOIN emp_hierarchy eh ON e.manager_id = eh.employee_id
    )
    SELECT
        eh.employee_id,
        eh.first_name || ' ' || eh.last_name AS employee_name,
        eh.manager_id,
        COALESCE(
            (SELECT m.first_name || ' ' || m.last_name FROM stg_employee m WHERE m.business_entity_id = eh.manager_id),
            NULL
        ) AS manager_name,
        eh.hierarchy_level,
        eh.manager_path,
        (SELECT e2.job_title FROM stg_employee e2 WHERE e2.business_entity_id = eh.employee_id) AS department,
        (SELECT COUNT(*) FROM stg_employee sub WHERE sub.manager_id = eh.employee_id) AS direct_reports_count
    FROM emp_hierarchy eh;

    COMMIT;
END;
/

-- ==========================================================
-- #14: usp_load_rpt_sales_by_category — TRUNCATE + INSERT
-- GROUPING SETS with GROUPING() for level identification
-- Joins fct_sales -> dim_product -> category hierarchy
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_sales_by_category
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_sales_by_category';

    INSERT INTO rpt_sales_by_category (category_name, subcategory_name, date_key, total_revenue, total_qty, grouping_level)
    SELECT
        NVL(dp.product_category, 'All Categories') AS category_name,
        dp.product_subcategory AS subcategory_name,
        fs.date_key,
        SUM(fs.line_total)  AS total_revenue,
        SUM(fs.order_qty)   AS total_qty,
        CASE
            WHEN GROUPING(dp.product_category) = 1 AND GROUPING(dp.product_subcategory) = 1 AND GROUPING(fs.date_key) = 1 THEN 3
            WHEN GROUPING(dp.product_subcategory) = 1 AND GROUPING(fs.date_key) = 1 THEN 2
            WHEN GROUPING(dp.product_subcategory) = 1 THEN 1
            ELSE 0
        END AS grouping_level
    FROM fct_sales fs
    INNER JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY GROUPING SETS (
        (dp.product_category, dp.product_subcategory, fs.date_key),
        (dp.product_category, fs.date_key),
        (dp.product_category),
        ()
    );

    COMMIT;
END;
/

-- ==========================================================
-- #15: usp_load_rpt_channel_pivot — TRUNCATE + INSERT
-- Native Oracle PIVOT and UNPIVOT syntax
-- UNPIVOT validation step compares row count
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_channel_pivot
AS
    v_pivot_count   NUMBER;
    v_unpivot_count NUMBER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_channel_pivot';

    -- PIVOT: narrow fct_sales_by_channel -> wide columns
    INSERT INTO rpt_channel_pivot (date_key, online_revenue, store_revenue, online_qty, store_qty,
                                    online_order_count, store_order_count)
    SELECT
        date_key,
        NVL(online_revenue, 0) AS online_revenue,
        NVL(store_revenue, 0)  AS store_revenue,
        NVL(online_qty, 0)     AS online_qty,
        NVL(store_qty, 0)      AS store_qty,
        NVL(online_order_count, 0) AS online_order_count,
        NVL(store_order_count, 0)  AS store_order_count
    FROM (
        SELECT date_key, channel, total_revenue, total_qty, order_count
        FROM fct_sales_by_channel
    )
    PIVOT (
        SUM(total_revenue) AS revenue,
        SUM(total_qty)     AS qty,
        SUM(order_count)   AS order_count
        FOR channel IN ('Online' AS online, 'Store' AS store)
    );

    SELECT COUNT(*) INTO v_pivot_count FROM rpt_channel_pivot;

    -- UNPIVOT validation: unpivot back to narrow and compare row count
    SELECT COUNT(*) INTO v_unpivot_count
    FROM (
        SELECT date_key, channel, metric_value
        FROM rpt_channel_pivot
        UNPIVOT (
            metric_value FOR channel IN (
                online_revenue AS 'Online_Revenue',
                store_revenue  AS 'Store_Revenue',
                online_qty     AS 'Online_Qty',
                store_qty      AS 'Store_Qty'
            )
        )
    );

    DBMS_OUTPUT.PUT_LINE('usp_load_rpt_channel_pivot: pivot_rows=' || v_pivot_count
                         || ' unpivot_rows=' || v_unpivot_count);

    COMMIT;
END;
/

-- ==========================================================
-- #16: usp_load_rpt_returns_analysis — TRUNCATE + INSERT
-- LEFT JOIN for return matching, NOT EXISTS, NOT IN with NULL guard,
-- IN subquery, HAVING for minimum threshold
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_returns_analysis
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_returns_analysis';

    INSERT INTO rpt_returns_analysis (product_key, product_name, date_key, sales_qty, return_qty,
                                      return_rate, top_return_reason)
    SELECT
        dp.product_key,
        dp.product_name,
        fs.date_key,
        SUM(fs.order_qty)                    AS sales_qty,
        NVL(SUM(r.return_qty), 0)            AS return_qty,
        CASE
            WHEN SUM(fs.order_qty) = 0 THEN 0
            ELSE ROUND(NVL(SUM(r.return_qty), 0) / SUM(fs.order_qty), 4)
        END AS return_rate,
        -- Scalar subquery for top return reason
        (SELECT rr.return_reason
         FROM stg_returns rr
         WHERE rr.sales_order_id = fs.sales_order_id
           AND rr.sales_order_detail_id = fs.sales_order_detail_id
         FETCH FIRST 1 ROW ONLY) AS top_return_reason
    FROM fct_sales fs
    INNER JOIN dim_product dp ON fs.product_key = dp.product_key
    -- LEFT JOIN for return matching
    LEFT JOIN stg_returns r
        ON fs.sales_order_id = r.sales_order_id
        AND fs.sales_order_detail_id = r.sales_order_detail_id
    -- NOT EXISTS: identify products that have at least some sales
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_product dp_disc
        WHERE dp_disc.product_key = dp.product_key
          AND dp_disc.sell_end_date IS NOT NULL
          AND dp_disc.is_current = 0
    )
    -- NOT IN with NULL guard for credit card exclusion
    AND fs.credit_card_key NOT IN (
        SELECT dcc.credit_card_key FROM dim_credit_card dcc
        WHERE dcc.exp_year < 2010
          AND dcc.credit_card_key IS NOT NULL
    )
    -- IN subquery for territory filter
    AND fs.sales_order_id IN (
        SELECT h.sales_order_id FROM stg_sales_order_header h
        WHERE h.territory_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    )
    GROUP BY dp.product_key, dp.product_name, fs.date_key, fs.sales_order_id, fs.sales_order_detail_id
    -- HAVING for minimum sales threshold
    HAVING SUM(fs.order_qty) >= 1;

    COMMIT;
END;
/

-- ==========================================================
-- #17: usp_load_rpt_customer_segments — TRUNCATE + INSERT
-- MINUS (Oracle EXCEPT), INTERSECT, UNION
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_customer_segments
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_customer_segments';

    INSERT INTO rpt_customer_segments (customer_id, segment_name, total_revenue, total_orders)
    -- UNION of three computed segments
    -- Segment 1: Active (all customers MINUS inactive)
    SELECT customer_id, 'Active' AS segment_name, total_revenue, total_orders
    FROM (
        SELECT
            dc.customer_id,
            NVL(SUM(fs.line_total), 0) AS total_revenue,
            COUNT(DISTINCT fs.sales_order_id) AS total_orders
        FROM dim_customer dc
        LEFT JOIN fct_sales fs ON dc.customer_key = fs.customer_key
        WHERE dc.is_current = 1
        GROUP BY dc.customer_id

        MINUS

        -- Inactive: customers with zero orders
        SELECT
            dc2.customer_id,
            0 AS total_revenue,
            0 AS total_orders
        FROM dim_customer dc2
        WHERE dc2.is_current = 1
          AND NOT EXISTS (
              SELECT 1 FROM fct_sales fs2 WHERE fs2.customer_key = dc2.customer_key
          )
    )

    UNION

    -- Segment 2: High-Value (active INTERSECT high-revenue)
    SELECT customer_id, 'High-Value' AS segment_name, total_revenue, total_orders
    FROM (
        SELECT
            dc.customer_id,
            SUM(fs.line_total) AS total_revenue,
            COUNT(DISTINCT fs.sales_order_id) AS total_orders
        FROM dim_customer dc
        INNER JOIN fct_sales fs ON dc.customer_key = fs.customer_key
        WHERE dc.is_current = 1
        GROUP BY dc.customer_id

        INTERSECT

        SELECT
            dc.customer_id,
            SUM(fs.line_total) AS total_revenue,
            COUNT(DISTINCT fs.sales_order_id) AS total_orders
        FROM dim_customer dc
        INNER JOIN fct_sales fs ON dc.customer_key = fs.customer_key
        WHERE dc.is_current = 1
        GROUP BY dc.customer_id
        HAVING SUM(fs.line_total) >= (SELECT AVG(total_due) FROM stg_sales_order_header)
    )

    UNION

    -- Segment 3: At-Risk (have orders but low recent activity)
    SELECT
        dc.customer_id,
        'At-Risk' AS segment_name,
        NVL(SUM(fs.line_total), 0) AS total_revenue,
        COUNT(DISTINCT fs.sales_order_id) AS total_orders
    FROM dim_customer dc
    INNER JOIN fct_sales fs ON dc.customer_key = fs.customer_key
    WHERE dc.is_current = 1
    GROUP BY dc.customer_id
    HAVING COUNT(DISTINCT fs.sales_order_id) = 1;

    COMMIT;
END;
/

-- ==========================================================
-- #18: usp_load_rpt_address_coverage — TRUNCATE + INSERT
-- FULL OUTER JOIN: stg_address vs dim_address
-- COALESCE, CASE for gap classification
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_rpt_address_coverage
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_address_coverage';

    INSERT INTO rpt_address_coverage (address_id, staging_city, dim_city, staging_postal_code,
                                      dim_postal_code, coverage_status)
    SELECT
        COALESCE(sa.address_id, da.address_id) AS address_id,
        sa.city          AS staging_city,
        da.city          AS dim_city,
        sa.postal_code   AS staging_postal_code,
        da.postal_code   AS dim_postal_code,
        CASE
            WHEN da.address_id IS NULL THEN 'new'
            WHEN sa.address_id IS NULL THEN 'orphan'
            ELSE 'matched'
        END AS coverage_status
    FROM stg_address sa
    FULL OUTER JOIN dim_address da
        ON sa.address_id = da.address_id AND da.is_current = 1;

    COMMIT;
END;
/

-- ==========================================================
-- #19: usp_load_gold_agg_batch — Multi-table via EXECUTE IMMEDIATE
-- CUBE aggregation -> rpt_product_margin
-- ROLLUP aggregation -> rpt_date_sales_rollup
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_load_gold_agg_batch
AS
BEGIN
    -- Truncate both target tables
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_product_margin';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_date_sales_rollup';

    -- CUBE aggregation: margin analysis by (product_line, category, color) -> rpt_product_margin
    EXECUTE IMMEDIATE '
        INSERT INTO rpt_product_margin (product_line, product_category, color,
                                        total_revenue, total_cost, total_margin,
                                        margin_pct, grouping_level)
        SELECT
            dp.product_line,
            dp.product_category,
            dp.color,
            SUM(fs.line_total)                                         AS total_revenue,
            SUM(fs.order_qty * dp.standard_cost)                       AS total_cost,
            SUM(fs.line_total) - SUM(fs.order_qty * dp.standard_cost)  AS total_margin,
            CASE
                WHEN SUM(fs.line_total) = 0 THEN 0
                ELSE ROUND((SUM(fs.line_total) - SUM(fs.order_qty * dp.standard_cost)) / SUM(fs.line_total), 4)
            END AS margin_pct,
            GROUPING_ID(dp.product_line, dp.product_category, dp.color) AS grouping_level
        FROM fct_sales fs
        INNER JOIN dim_product dp ON fs.product_key = dp.product_key
        GROUP BY CUBE(dp.product_line, dp.product_category, dp.color)
    ';

    -- ROLLUP aggregation: date hierarchy (year, quarter, month) -> rpt_date_sales_rollup
    EXECUTE IMMEDIATE '
        INSERT INTO rpt_date_sales_rollup (year_number, quarter_number, month_number,
                                            total_revenue, total_qty, order_count, rollup_level)
        SELECT
            NVL(dd.year_number, 0)    AS year_number,
            dd.quarter_number,
            dd.month_number,
            SUM(fs.line_total)        AS total_revenue,
            SUM(fs.order_qty)         AS total_qty,
            COUNT(*)                  AS order_count,
            GROUPING_ID(dd.year_number, dd.quarter_number, dd.month_number) AS rollup_level
        FROM fct_sales fs
        INNER JOIN dim_date dd ON fs.date_key = dd.date_key
        GROUP BY ROLLUP(dd.year_number, dd.quarter_number, dd.month_number)
    ';

    COMMIT;
END;
/

-- ==========================================================
-- #20: usp_exec_orchestrator_full_load — Exec orchestrator
-- Calls procs in dependency order using all EXEC variants:
-- direct call, EXECUTE IMMEDIATE with params, OUT param, return, dynamic
-- ==========================================================

CREATE OR REPLACE PROCEDURE usp_exec_orchestrator_full_load
AS
    v_row_count NUMBER := 0;
    v_result    NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Orchestrator: starting full load');

    -- 1. Direct call to usp_load_dim_customer
    usp_load_dim_customer;
    DBMS_OUTPUT.PUT_LINE('Orchestrator: dim_customer loaded');

    -- 2. EXECUTE IMMEDIATE with params (mode = SCD1)
    EXECUTE IMMEDIATE 'BEGIN usp_load_dim_employee; END;';
    DBMS_OUTPUT.PUT_LINE('Orchestrator: dim_employee loaded (SCD1)');

    -- 3. Direct call with row count capture (simulates OUTPUT param)
    usp_load_fct_sales_daily(p_mode => 'FULL');
    SELECT COUNT(*) INTO v_row_count FROM fct_sales;
    DBMS_OUTPUT.PUT_LINE('Orchestrator: fct_sales_daily loaded, rows=' || v_row_count);

    -- 4. Direct call with return value simulation
    usp_load_fct_sales_summary;
    SELECT COUNT(*) INTO v_result FROM fct_sales_summary;
    DBMS_OUTPUT.PUT_LINE('Orchestrator: fct_sales_summary loaded, rows=' || v_result);

    -- 5. EXECUTE IMMEDIATE for dynamic string call
    EXECUTE IMMEDIATE 'BEGIN usp_load_gold_agg_batch; END;';
    DBMS_OUTPUT.PUT_LINE('Orchestrator: gold_agg_batch loaded');

    DBMS_OUTPUT.PUT_LINE('Orchestrator: full load complete');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Orchestrator ERROR at step: ' || SQLERRM);
        RAISE;
END;
/

-- ==========================================================
-- #21: usp_load_dim_address_and_credit_card is procedure #5
-- (counted as #21 because the inventory lists 21 total procs
--  including the helper usp_enrich_customer_address)
-- ==========================================================
-- All 21 procedures created:
--   Helper: usp_enrich_customer_address
--   #1:  usp_load_dim_customer
--   #2:  usp_load_dim_product
--   #3:  usp_load_dim_employee
--   #4:  usp_load_dim_product_category
--   #5:  usp_load_dim_address_and_credit_card
--   #6:  usp_load_fct_sales_daily
--   #7:  usp_load_fct_sales_historical
--   #8:  usp_load_fct_sales_summary
--   #9:  usp_load_fct_sales_by_channel
--   #10: usp_load_rpt_customer_lifetime_value
--   #11: usp_load_rpt_product_performance
--   #12: usp_load_rpt_sales_by_territory
--   #13: usp_load_rpt_employee_hierarchy
--   #14: usp_load_rpt_sales_by_category
--   #15: usp_load_rpt_channel_pivot
--   #16: usp_load_rpt_returns_analysis
--   #17: usp_load_rpt_customer_segments
--   #18: usp_load_rpt_address_coverage
--   #19: usp_load_gold_agg_batch
--   #20: usp_exec_orchestrator_full_load
-- ==========================================================
