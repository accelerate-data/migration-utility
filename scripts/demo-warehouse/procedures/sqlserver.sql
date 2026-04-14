-- ==========================================================
-- Kimball DW Fixture — SQL Server Stored Procedures
-- Self-contained, idempotent: drops all procs first, then creates
-- ==========================================================

USE KimballFixture;
GO

-- ----------------------------------------------------------
-- Drop all procedures (reverse dependency order)
-- ----------------------------------------------------------
IF OBJECT_ID('dbo.usp_exec_orchestrator_full_load', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_exec_orchestrator_full_load;
GO
IF OBJECT_ID('dbo.usp_load_gold_agg_batch', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_gold_agg_batch;
GO
IF OBJECT_ID('dbo.usp_load_rpt_address_coverage', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_address_coverage;
GO
IF OBJECT_ID('dbo.usp_load_rpt_customer_segments', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_customer_segments;
GO
IF OBJECT_ID('dbo.usp_load_rpt_returns_analysis', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_returns_analysis;
GO
IF OBJECT_ID('dbo.usp_load_rpt_channel_pivot', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_channel_pivot;
GO
IF OBJECT_ID('dbo.usp_load_rpt_sales_by_category', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_sales_by_category;
GO
IF OBJECT_ID('dbo.usp_load_rpt_employee_hierarchy', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_employee_hierarchy;
GO
IF OBJECT_ID('dbo.usp_load_rpt_sales_by_territory', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_sales_by_territory;
GO
IF OBJECT_ID('dbo.usp_load_rpt_product_performance', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_product_performance;
GO
IF OBJECT_ID('dbo.usp_load_rpt_customer_lifetime_value', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_rpt_customer_lifetime_value;
GO
IF OBJECT_ID('dbo.usp_load_fct_sales_by_channel', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_fct_sales_by_channel;
GO
IF OBJECT_ID('dbo.usp_load_fct_sales_summary', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_fct_sales_summary;
GO
IF OBJECT_ID('dbo.usp_load_fct_sales_historical', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_fct_sales_historical;
GO
IF OBJECT_ID('dbo.usp_load_fct_sales_daily', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_fct_sales_daily;
GO
IF OBJECT_ID('dbo.usp_load_dim_address_and_credit_card', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_dim_address_and_credit_card;
GO
IF OBJECT_ID('dbo.usp_load_dim_product_category', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_dim_product_category;
GO
IF OBJECT_ID('dbo.usp_load_dim_employee', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_dim_employee;
GO
IF OBJECT_ID('dbo.usp_load_dim_product', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_dim_product;
GO
IF OBJECT_ID('dbo.usp_load_dim_customer', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_load_dim_customer;
GO

-- ==========================================================
-- Category: Dimension loads (#1-5)
-- ==========================================================

-- ----------------------------------------------------------
-- #1 usp_load_dim_customer
-- Target: dim.dim_customer
-- Pattern: MERGE SCD2, CTE (view-backed), CASE, EXEC call
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    -- Side-effect: refresh product category dim before customer load
    EXEC dbo.usp_load_dim_product_category;

    WITH src AS (
        SELECT
            vc.customer_id,
            vc.person_id,
            vc.store_id,
            vc.territory_id,
            CASE
                WHEN vc.full_name IS NOT NULL AND LEN(vc.full_name) > 0
                    THEN vc.full_name
                WHEN p.first_name IS NOT NULL
                    THEN CONCAT(p.first_name, ' ', COALESCE(p.middle_name + ' ', ''), p.last_name)
                ELSE N'Unknown'
            END AS full_name,
            vc.modified_date
        FROM staging.vw_stg_customer vc
        LEFT JOIN staging.stg_person p ON vc.person_id = p.business_entity_id
    )
    MERGE dim.dim_customer AS tgt
    USING src
        ON tgt.customer_id = src.customer_id
        AND tgt.is_current = 1
    WHEN MATCHED AND (
        ISNULL(tgt.full_name, N'') <> ISNULL(src.full_name, N'')
        OR ISNULL(tgt.store_id, -1) <> ISNULL(src.store_id, -1)
        OR ISNULL(tgt.territory_id, -1) <> ISNULL(src.territory_id, -1)
    ) THEN
        UPDATE SET
            tgt.valid_to = GETDATE(),
            tgt.is_current = 0
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (customer_id, person_id, store_id, full_name, territory_id, valid_from, valid_to, is_current)
        VALUES (src.customer_id, src.person_id, src.store_id, src.full_name, src.territory_id, GETDATE(), NULL, 1);

    -- Re-insert changed rows as new current records
    INSERT INTO dim.dim_customer (customer_id, person_id, store_id, full_name, territory_id, valid_from, valid_to, is_current)
    SELECT
        src.customer_id,
        src.person_id,
        src.store_id,
        src.full_name,
        src.territory_id,
        GETDATE(),
        NULL,
        1
    FROM (
        SELECT
            vc.customer_id,
            vc.person_id,
            vc.store_id,
            vc.territory_id,
            CASE
                WHEN vc.full_name IS NOT NULL AND LEN(vc.full_name) > 0
                    THEN vc.full_name
                WHEN p.first_name IS NOT NULL
                    THEN CONCAT(p.first_name, ' ', COALESCE(p.middle_name + ' ', ''), p.last_name)
                ELSE N'Unknown'
            END AS full_name
        FROM staging.vw_stg_customer vc
        LEFT JOIN staging.stg_person p ON vc.person_id = p.business_entity_id
    ) src
    INNER JOIN dim.dim_customer exp
        ON exp.customer_id = src.customer_id
        AND exp.is_current = 0
        AND exp.valid_to >= DATEADD(SECOND, -5, GETDATE())
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_customer cur
        WHERE cur.customer_id = src.customer_id
        AND cur.is_current = 1
    );
END;
GO

-- ----------------------------------------------------------
-- #2 usp_load_dim_product
-- Target: dim.dim_product
-- Pattern: MERGE SCD2, multi-level CTE, INNER JOINs
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_product
AS
BEGIN
    SET NOCOUNT ON;

    WITH product_base AS (
        SELECT
            p.product_id,
            p.product_name,
            p.product_number,
            p.color,
            p.class,
            p.product_line,
            p.standard_cost,
            p.list_price,
            p.sell_start_date,
            p.sell_end_date,
            p.product_subcategory_id
        FROM staging.stg_product p
    ),
    product_with_subcategory AS (
        SELECT
            pb.*,
            sc.subcategory_name AS product_subcategory,
            sc.product_category_id
        FROM product_base pb
        INNER JOIN staging.stg_product_subcategory sc
            ON pb.product_subcategory_id = sc.product_subcategory_id
    ),
    product_enriched AS (
        SELECT
            ps.*,
            cat.category_name AS product_category
        FROM product_with_subcategory ps
        INNER JOIN staging.stg_product_category cat
            ON ps.product_category_id = cat.product_category_id
    )
    MERGE dim.dim_product AS tgt
    USING product_enriched AS src
        ON tgt.product_id = src.product_id
        AND tgt.is_current = 1
    WHEN MATCHED AND (
        tgt.product_name <> src.product_name
        OR tgt.standard_cost <> src.standard_cost
        OR tgt.list_price <> src.list_price
        OR ISNULL(tgt.color, N'') <> ISNULL(src.color, N'')
        OR ISNULL(tgt.product_subcategory, N'') <> ISNULL(src.product_subcategory, N'')
        OR ISNULL(tgt.product_category, N'') <> ISNULL(src.product_category, N'')
    ) THEN
        UPDATE SET
            tgt.valid_to = GETDATE(),
            tgt.is_current = 0
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (product_id, product_name, product_number, color, class, product_line,
                standard_cost, list_price, product_subcategory, product_category,
                sell_start_date, sell_end_date, valid_from, valid_to, is_current)
        VALUES (src.product_id, src.product_name, src.product_number, src.color, src.class, src.product_line,
                src.standard_cost, src.list_price, src.product_subcategory, src.product_category,
                src.sell_start_date, src.sell_end_date, GETDATE(), NULL, 1);

    -- Insert new current rows for expired records
    INSERT INTO dim.dim_product (product_id, product_name, product_number, color, class, product_line,
                                  standard_cost, list_price, product_subcategory, product_category,
                                  sell_start_date, sell_end_date, valid_from, valid_to, is_current)
    SELECT
        pe.product_id, pe.product_name, pe.product_number, pe.color, pe.class, pe.product_line,
        pe.standard_cost, pe.list_price, pe.product_subcategory, pe.product_category,
        pe.sell_start_date, pe.sell_end_date, GETDATE(), NULL, 1
    FROM (
        SELECT
            p.product_id, p.product_name, p.product_number, p.color, p.class, p.product_line,
            p.standard_cost, p.list_price, p.sell_start_date, p.sell_end_date,
            sc.subcategory_name AS product_subcategory,
            cat.category_name AS product_category
        FROM staging.stg_product p
        INNER JOIN staging.stg_product_subcategory sc ON p.product_subcategory_id = sc.product_subcategory_id
        INNER JOIN staging.stg_product_category cat ON sc.product_category_id = cat.product_category_id
    ) pe
    INNER JOIN dim.dim_product exp
        ON exp.product_id = pe.product_id
        AND exp.is_current = 0
        AND exp.valid_to >= DATEADD(SECOND, -5, GETDATE())
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.dim_product cur
        WHERE cur.product_id = pe.product_id AND cur.is_current = 1
    );
END;
GO

-- ----------------------------------------------------------
-- #3 usp_load_dim_employee
-- Target: dim.dim_employee
-- Pattern: MERGE SCD1 (upsert), self-join, COALESCE, scalar subquery
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_employee
    @mode NVARCHAR(10) = N'SCD1'
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dim.dim_employee AS tgt
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
            COALESCE(
                CONCAT(m.first_name, ' ', m.last_name),
                N'No Manager'
            ) AS manager_name,
            (SELECT COUNT(*)
             FROM staging.stg_employee dr
             WHERE dr.manager_id = e.business_entity_id) AS direct_reports_count
        FROM staging.stg_employee e
        LEFT JOIN staging.stg_employee m
            ON e.manager_id = m.business_entity_id
    ) AS src
        ON tgt.employee_id = src.employee_id
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
            tgt.valid_from         = GETDATE(),
            tgt.is_current         = 1
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (employee_id, national_id_number, first_name, last_name, job_title,
                birth_date, gender, hire_date, salaried_flag, current_flag,
                valid_from, valid_to, is_current)
        VALUES (src.employee_id, src.national_id_number, src.first_name, src.last_name, src.job_title,
                src.birth_date, src.gender, src.hire_date, src.salaried_flag, src.current_flag,
                GETDATE(), NULL, 1);
END;
GO

-- ----------------------------------------------------------
-- #4 usp_load_dim_product_category
-- Target: dim.dim_product_category
-- Pattern: MERGE SCD2, EXISTS change detection
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_product_category
AS
BEGIN
    SET NOCOUNT ON;

    -- Expire rows where source has changed
    UPDATE dim.dim_product_category
    SET valid_to = GETDATE(),
        is_current = 0
    WHERE is_current = 1
    AND EXISTS (
        SELECT 1
        FROM staging.stg_product_category src
        WHERE src.product_category_id = dim.dim_product_category.product_category_id
        AND src.category_name <> dim.dim_product_category.category_name
    );

    -- MERGE for inserts and new current rows
    MERGE dim.dim_product_category AS tgt
    USING staging.stg_product_category AS src
        ON tgt.product_category_id = src.product_category_id
        AND tgt.is_current = 1
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (product_category_id, category_name, valid_from, valid_to, is_current)
        VALUES (src.product_category_id, src.category_name, GETDATE(), NULL, 1);

    -- Insert new current rows for expired categories
    INSERT INTO dim.dim_product_category (product_category_id, category_name, valid_from, valid_to, is_current)
    SELECT
        src.product_category_id,
        src.category_name,
        GETDATE(),
        NULL,
        1
    FROM staging.stg_product_category src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim.dim_product_category cur
        WHERE cur.product_category_id = src.product_category_id
        AND cur.is_current = 1
    );
END;
GO

-- ----------------------------------------------------------
-- #5 usp_load_dim_address_and_credit_card
-- Target: dim.dim_address + dim.dim_credit_card
-- Pattern: Multi-table MERGE, OUTER APPLY
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_address_and_credit_card
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================
    -- MERGE 1: dim_address (SCD2)
    -- ==============================
    MERGE dim.dim_address AS tgt
    USING staging.stg_address AS src
        ON tgt.address_id = src.address_id
        AND tgt.is_current = 1
    WHEN MATCHED AND (
        tgt.address_line_1 <> src.address_line_1
        OR tgt.city <> src.city
        OR tgt.postal_code <> src.postal_code
        OR tgt.state_province_id <> src.state_province_id
    ) THEN
        UPDATE SET
            tgt.valid_to = GETDATE(),
            tgt.is_current = 0
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
        VALUES (src.address_id, src.address_line_1, src.city, src.state_province_id, src.postal_code, GETDATE(), NULL, 1);

    -- Re-insert expired address rows as current
    INSERT INTO dim.dim_address (address_id, address_line_1, city, state_province_id, postal_code, valid_from, valid_to, is_current)
    SELECT
        src.address_id, src.address_line_1, src.city, src.state_province_id, src.postal_code, GETDATE(), NULL, 1
    FROM staging.stg_address src
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.dim_address cur
        WHERE cur.address_id = src.address_id AND cur.is_current = 1
    )
    AND EXISTS (
        SELECT 1 FROM dim.dim_address old
        WHERE old.address_id = src.address_id AND old.is_current = 0
    );

    -- ==============================
    -- MERGE 2: dim_credit_card (SCD2)
    -- Uses OUTER APPLY to correlate latest credit card per address via order headers
    -- ==============================
    MERGE dim.dim_credit_card AS tgt
    USING (
        SELECT DISTINCT
            cc.credit_card_id,
            cc.card_type,
            cc.exp_month,
            cc.exp_year
        FROM staging.stg_credit_card cc
        OUTER APPLY (
            SELECT TOP 1 h.bill_to_address_id
            FROM staging.stg_sales_order_header h
            WHERE h.credit_card_id = cc.credit_card_id
            ORDER BY h.order_date DESC
        ) latest_addr
    ) AS src
        ON tgt.credit_card_id = src.credit_card_id
        AND tgt.is_current = 1
    WHEN MATCHED AND (
        tgt.card_type <> src.card_type
        OR tgt.exp_month <> src.exp_month
        OR tgt.exp_year <> src.exp_year
    ) THEN
        UPDATE SET
            tgt.valid_to = GETDATE(),
            tgt.is_current = 0
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
        VALUES (src.credit_card_id, src.card_type, src.exp_month, src.exp_year, GETDATE(), NULL, 1);

    -- Re-insert expired credit card rows as current
    INSERT INTO dim.dim_credit_card (credit_card_id, card_type, exp_month, exp_year, valid_from, valid_to, is_current)
    SELECT
        src.credit_card_id, src.card_type, src.exp_month, src.exp_year, GETDATE(), NULL, 1
    FROM staging.stg_credit_card src
    WHERE NOT EXISTS (
        SELECT 1 FROM dim.dim_credit_card cur
        WHERE cur.credit_card_id = src.credit_card_id AND cur.is_current = 1
    )
    AND EXISTS (
        SELECT 1 FROM dim.dim_credit_card old
        WHERE old.credit_card_id = src.credit_card_id AND old.is_current = 0
    );
END;
GO

-- ==========================================================
-- Category: Fact loads (#6-9)
-- ==========================================================

-- ----------------------------------------------------------
-- #6 usp_load_fct_sales_daily
-- Target: fact.fct_sales (incremental)
-- Pattern: INSERT...SELECT, multi-JOIN, EXISTS filter, IF/ELSE,
--          TRY/CATCH, derived table, correlated subquery, CROSS APPLY
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_fct_sales_daily
    @mode NVARCHAR(20) = N'INCREMENTAL',
    @load_date DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows_inserted INT = 0;

    IF @load_date IS NULL
        SET @load_date = GETDATE();

    BEGIN TRY
        IF @mode = N'FULL'
        BEGIN
            TRUNCATE TABLE fact.fct_sales;

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
                CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
                da.address_key,
                dcc.credit_card_key,
                dos.order_status_key,
                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total
            FROM staging.stg_sales_order_header h
            INNER JOIN staging.stg_sales_order_detail d
                ON h.sales_order_id = d.sales_order_id
            INNER JOIN dim.dim_customer dc
                ON h.customer_id = dc.customer_id AND dc.is_current = 1
            INNER JOIN dim.dim_product dp
                ON d.product_id = dp.product_id AND dp.is_current = 1
            INNER JOIN dim.dim_address da
                ON h.bill_to_address_id = da.address_id AND da.is_current = 1
            INNER JOIN dim.dim_order_status dos
                ON h.status = dos.order_status
            LEFT JOIN dim.dim_credit_card dcc
                ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
            CROSS APPLY (
                SELECT TOP 3 od.line_total AS top_line_total
                FROM staging.stg_sales_order_detail od
                WHERE od.sales_order_id = h.sales_order_id
                ORDER BY od.line_total DESC
            ) top_orders
            WHERE d.unit_price >= (
                SELECT AVG(sub_d.unit_price) * 0.01
                FROM staging.stg_sales_order_detail sub_d
                WHERE sub_d.product_id = d.product_id
            )
            AND d.sales_order_detail_id = (
                SELECT MIN(od2.sales_order_detail_id)
                FROM staging.stg_sales_order_detail od2
                WHERE od2.sales_order_id = d.sales_order_id
                    AND od2.sales_order_detail_id = d.sales_order_detail_id
            );

            SET @rows_inserted = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            -- Incremental: only new rows since @load_date
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
                CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
                da.address_key,
                dcc.credit_card_key,
                dos.order_status_key,
                d.order_qty,
                d.unit_price,
                d.unit_price_discount,
                d.line_total
            FROM staging.stg_sales_order_header h
            INNER JOIN staging.stg_sales_order_detail d
                ON h.sales_order_id = d.sales_order_id
            INNER JOIN dim.dim_customer dc
                ON h.customer_id = dc.customer_id AND dc.is_current = 1
            INNER JOIN dim.dim_product dp
                ON d.product_id = dp.product_id AND dp.is_current = 1
            INNER JOIN dim.dim_address da
                ON h.bill_to_address_id = da.address_id AND da.is_current = 1
            INNER JOIN dim.dim_order_status dos
                ON h.status = dos.order_status
            LEFT JOIN dim.dim_credit_card dcc
                ON h.credit_card_id = dcc.credit_card_id AND dcc.is_current = 1
            CROSS APPLY (
                SELECT TOP 3 od.line_total AS top_line_total
                FROM staging.stg_sales_order_detail od
                WHERE od.sales_order_id = h.sales_order_id
                ORDER BY od.line_total DESC
            ) top_orders
            WHERE h.order_date >= DATEADD(DAY, -1, @load_date)
            AND NOT EXISTS (
                SELECT 1 FROM fact.fct_sales f
                WHERE f.sales_order_id = h.sales_order_id
                AND f.sales_order_detail_id = d.sales_order_detail_id
            )
            AND d.unit_price >= (
                SELECT AVG(sub_d.unit_price) * 0.01
                FROM staging.stg_sales_order_detail sub_d
                WHERE sub_d.product_id = d.product_id
            )
            AND d.sales_order_detail_id = (
                SELECT MIN(od2.sales_order_detail_id)
                FROM staging.stg_sales_order_detail od2
                WHERE od2.sales_order_id = d.sales_order_id
                    AND od2.sales_order_detail_id = d.sales_order_detail_id
            );

            SET @rows_inserted = @@ROWCOUNT;
        END;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @err_sev INT = ERROR_SEVERITY();
        DECLARE @err_state INT = ERROR_STATE();
        RAISERROR(@err_msg, @err_sev, @err_state);
        RETURN -1;
    END CATCH;

    RETURN @rows_inserted;
END;
GO

-- ----------------------------------------------------------
-- #7 usp_load_fct_sales_historical
-- Target: fact.fct_sales (full rebuild) — CONFLICT with #6
-- Pattern: TRUNCATE+INSERT, intentionally ugly/messy, WHILE batch loop,
--          nested IF/ELSE + WHILE, TRY/CATCH
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_fct_sales_historical
    @batch_size INT = 1000
AS
BEGIN
SET NOCOUNT ON;
DECLARE @total_rows INT, @offset INT, @threshold INT;
SET @threshold = 500;
SET @offset = 0;

BEGIN TRY
TRUNCATE TABLE fact.fct_sales;

-- count total rows to decide batching strategy
SELECT @total_rows = COUNT(*) FROM staging.stg_sales_order_detail;

IF @total_rows > @threshold
BEGIN
-- ugly batch loop: chunks of @batch_size
WHILE @offset < @total_rows
BEGIN
INSERT INTO fact.fct_sales (sales_order_id,sales_order_detail_id,customer_key,product_key,date_key,address_key,credit_card_key,order_status_key,order_qty,unit_price,unit_price_discount,line_total)
SELECT sub.sales_order_id,sub.sales_order_detail_id,sub.customer_key,sub.product_key,sub.date_key,sub.address_key,sub.credit_card_key,sub.order_status_key,sub.order_qty,sub.unit_price,sub.unit_price_discount,sub.line_total
FROM (
SELECT h.sales_order_id,d.sales_order_detail_id,dc.customer_key,dp.product_key,CAST(CONVERT(VARCHAR(8),h.order_date,112) AS INT) AS date_key,da.address_key,dcc.credit_card_key,dos.order_status_key,d.order_qty,d.unit_price,d.unit_price_discount,d.line_total,
ROW_NUMBER() OVER (ORDER BY h.sales_order_id, d.sales_order_detail_id) AS _rn
FROM staging.stg_sales_order_header h INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id=d.sales_order_id INNER JOIN dim.dim_customer dc ON h.customer_id=dc.customer_id AND dc.is_current=1 INNER JOIN dim.dim_product dp ON d.product_id=dp.product_id AND dp.is_current=1 INNER JOIN dim.dim_address da ON h.bill_to_address_id=da.address_id AND da.is_current=1 INNER JOIN dim.dim_order_status dos ON h.status=dos.order_status LEFT JOIN dim.dim_credit_card dcc ON h.credit_card_id=dcc.credit_card_id AND dcc.is_current=1
) sub
WHERE sub._rn > @offset AND sub._rn <= @offset + @batch_size;

SET @offset = @offset + @batch_size;
END; -- end while
END
ELSE
BEGIN
-- single-pass insert for small datasets
INSERT INTO fact.fct_sales (sales_order_id,sales_order_detail_id,customer_key,product_key,date_key,address_key,credit_card_key,order_status_key,order_qty,unit_price,unit_price_discount,line_total)
SELECT h.sales_order_id,d.sales_order_detail_id,dc.customer_key,dp.product_key,CAST(CONVERT(VARCHAR(8),h.order_date,112) AS INT) AS date_key,da.address_key,dcc.credit_card_key,dos.order_status_key,d.order_qty,d.unit_price,d.unit_price_discount,d.line_total
FROM staging.stg_sales_order_header h INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id=d.sales_order_id INNER JOIN dim.dim_customer dc ON h.customer_id=dc.customer_id AND dc.is_current=1 INNER JOIN dim.dim_product dp ON d.product_id=dp.product_id AND dp.is_current=1 INNER JOIN dim.dim_address da ON h.bill_to_address_id=da.address_id AND da.is_current=1 INNER JOIN dim.dim_order_status dos ON h.status=dos.order_status LEFT JOIN dim.dim_credit_card dcc ON h.credit_card_id=dcc.credit_card_id AND dcc.is_current=1;
END;

END TRY
BEGIN CATCH
DECLARE @errmsg NVARCHAR(4000)=ERROR_MESSAGE();
RAISERROR(@errmsg, 16, 1);
RETURN -1;
END CATCH;
END;
GO

-- ----------------------------------------------------------
-- #8 usp_load_fct_sales_summary
-- Target: fact.fct_sales_summary
-- Pattern: TRUNCATE+INSERT, ROLLUP, GROUPING()
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_fct_sales_summary
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE fact.fct_sales_summary;

    INSERT INTO fact.fct_sales_summary (date_key, product_key, total_qty, total_revenue, order_count)
    SELECT
        COALESCE(dd.date_key, 0) AS date_key,
        COALESCE(f.product_key, 0) AS product_key,
        SUM(f.order_qty) AS total_qty,
        SUM(f.line_total) AS total_revenue,
        COUNT(DISTINCT f.sales_order_id) AS order_count
    FROM fact.fct_sales f
    INNER JOIN dim.dim_date dd
        ON f.date_key = dd.date_key
    GROUP BY ROLLUP (dd.year_number, dd.quarter_number, dd.month_number),
             f.product_key,
             dd.date_key
    HAVING SUM(f.order_qty) > 0
       OR GROUPING(dd.year_number) = 1;
END;
GO

-- ----------------------------------------------------------
-- #9 usp_load_fct_sales_by_channel
-- Target: fact.fct_sales_by_channel
-- Pattern: TRUNCATE+INSERT, CTE with UNION ALL, aggregation
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_fct_sales_by_channel
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE fact.fct_sales_by_channel;

    WITH channel_split AS (
        SELECT
            CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
            N'Online' AS channel,
            d.order_qty,
            d.line_total,
            h.sales_order_id
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = 1

        UNION ALL

        SELECT
            CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
            N'Store' AS channel,
            d.order_qty,
            d.line_total,
            h.sales_order_id
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail d ON h.sales_order_id = d.sales_order_id
        WHERE h.online_order_flag = 0
    )
    INSERT INTO fact.fct_sales_by_channel (date_key, channel, total_qty, total_revenue, order_count)
    SELECT
        cs.date_key,
        cs.channel,
        SUM(cs.order_qty) AS total_qty,
        SUM(cs.line_total) AS total_revenue,
        COUNT(DISTINCT cs.sales_order_id) AS order_count
    FROM channel_split cs
    GROUP BY cs.date_key, cs.channel;
END;
GO

-- ==========================================================
-- Category: Gold-layer loads (#10-18)
-- ==========================================================

-- ----------------------------------------------------------
-- #10 usp_load_rpt_customer_lifetime_value
-- Target: gold.rpt_customer_lifetime_value
-- Pattern: TRUNCATE+INSERT, view-backed (vw_customer_360),
--          RANK(), NTILE(4), scalar subquery
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_customer_lifetime_value
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_customer_lifetime_value;

    INSERT INTO gold.rpt_customer_lifetime_value (
        customer_key, customer_id, full_name, total_orders, total_revenue,
        avg_order_value, first_order_date, last_order_date, customer_tier, revenue_quartile
    )
    SELECT
        dc.customer_key,
        v.customer_id,
        v.full_name,
        v.total_orders,
        v.total_revenue,
        (SELECT AVG(h.total_due)
         FROM staging.stg_sales_order_header h
         WHERE h.customer_id = v.customer_id) AS avg_order_value,
        v.first_order_date,
        v.last_order_date,
        v.customer_tier,
        NTILE(4) OVER (ORDER BY v.total_revenue DESC) AS revenue_quartile
    FROM staging.vw_customer_360 v
    INNER JOIN dim.dim_customer dc
        ON v.customer_id = dc.customer_id
        AND dc.is_current = 1;
END;
GO

-- ----------------------------------------------------------
-- #11 usp_load_rpt_product_performance
-- Target: gold.rpt_product_performance
-- Pattern: TRUNCATE+INSERT, view-backed (vw_enriched_sales),
--          multi-level CTE, LAG, RANK, CASE for trend
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_product_performance
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_product_performance;

    WITH monthly_agg AS (
        SELECT
            dp.product_key,
            dp.product_name,
            dd.year_number * 100 + dd.month_number AS year_month_key,
            MIN(dd.date_key) AS date_key,
            SUM(es.line_total) AS monthly_revenue,
            SUM(es.order_qty) AS monthly_qty
        FROM staging.vw_enriched_sales es
        INNER JOIN dim.dim_product dp
            ON es.product_id = dp.product_id AND dp.is_current = 1
        INNER JOIN dim.dim_date dd
            ON es.date_key = dd.date_key
        GROUP BY dp.product_key, dp.product_name,
                 dd.year_number * 100 + dd.month_number
    ),
    with_lag AS (
        SELECT
            ma.*,
            LAG(ma.monthly_revenue) OVER (
                PARTITION BY ma.product_key
                ORDER BY ma.year_month_key
            ) AS prev_monthly_revenue
        FROM monthly_agg ma
    ),
    with_rank AS (
        SELECT
            wl.*,
            RANK() OVER (
                PARTITION BY wl.year_month_key
                ORDER BY wl.monthly_revenue DESC
            ) AS revenue_rank,
            CASE
                WHEN wl.prev_monthly_revenue IS NULL OR wl.prev_monthly_revenue = 0 THEN NULL
                ELSE CAST((wl.monthly_revenue - wl.prev_monthly_revenue) * 100.0 / wl.prev_monthly_revenue AS DECIMAL(10,4))
            END AS mom_growth_pct
        FROM with_lag wl
    )
    INSERT INTO gold.rpt_product_performance (
        product_key, product_name, date_key, monthly_revenue, monthly_qty,
        revenue_rank, mom_growth_pct, trend
    )
    SELECT
        wr.product_key,
        wr.product_name,
        wr.date_key,
        wr.monthly_revenue,
        wr.monthly_qty,
        wr.revenue_rank,
        wr.mom_growth_pct,
        CASE
            WHEN wr.mom_growth_pct IS NULL THEN N'New'
            WHEN wr.mom_growth_pct > 10.0 THEN N'Growth'
            WHEN wr.mom_growth_pct < -10.0 THEN N'Decline'
            ELSE N'Stable'
        END AS trend
    FROM with_rank wr;
END;
GO

-- ----------------------------------------------------------
-- #12 usp_load_rpt_sales_by_territory
-- Target: gold.rpt_sales_by_territory
-- Pattern: TRUNCATE+INSERT, CROSS JOIN scaffold, LEFT JOIN,
--          RIGHT JOIN, RANK()
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_sales_by_territory
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_sales_by_territory;

    INSERT INTO gold.rpt_sales_by_territory (
        territory_id, date_key, total_revenue, total_qty, order_count, territory_rank
    )
    SELECT
        scaffold.territory_id,
        scaffold.date_key,
        ISNULL(agg.total_revenue, 0) AS total_revenue,
        ISNULL(agg.total_qty, 0) AS total_qty,
        ISNULL(agg.order_count, 0) AS order_count,
        RANK() OVER (
            PARTITION BY scaffold.date_key
            ORDER BY ISNULL(agg.total_revenue, 0) DESC
        ) AS territory_rank
    FROM (
        -- CROSS JOIN scaffold: all territory + date combos
        SELECT t.territory_id, d.date_key
        FROM (SELECT DISTINCT territory_id FROM staging.stg_sales_order_header WHERE territory_id IS NOT NULL) t
        CROSS JOIN (SELECT DISTINCT date_key FROM dim.dim_date) d
    ) scaffold
    LEFT JOIN (
        SELECT
            h.territory_id,
            CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT) AS date_key,
            SUM(det.line_total) AS total_revenue,
            SUM(det.order_qty) AS total_qty,
            COUNT(DISTINCT h.sales_order_id) AS order_count
        FROM staging.stg_sales_order_header h
        INNER JOIN staging.stg_sales_order_detail det ON h.sales_order_id = det.sales_order_id
        WHERE h.territory_id IS NOT NULL
        GROUP BY h.territory_id, CAST(CONVERT(VARCHAR(8), h.order_date, 112) AS INT)
    ) agg
        ON scaffold.territory_id = agg.territory_id
        AND scaffold.date_key = agg.date_key

    -- RIGHT JOIN variant: ensure all territories from header appear
    -- (already covered via scaffold, but included for pattern coverage)
    RIGHT JOIN (
        SELECT DISTINCT territory_id
        FROM staging.stg_sales_order_header
        WHERE territory_id IS NOT NULL
    ) terr_coverage
        ON scaffold.territory_id = terr_coverage.territory_id;
END;
GO

-- ----------------------------------------------------------
-- #13 usp_load_rpt_employee_hierarchy
-- Target: gold.rpt_employee_hierarchy
-- Pattern: TRUNCATE+INSERT, recursive CTE, scalar subquery,
--          string concatenation for manager_path
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_employee_hierarchy
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_employee_hierarchy;

    WITH hierarchy AS (
        -- Anchor: top-level employees (no manager)
        SELECT
            e.business_entity_id AS employee_id,
            CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
            e.manager_id,
            CAST(NULL AS NVARCHAR(101)) AS manager_name,
            1 AS hierarchy_level,
            CAST(CONCAT(e.first_name, ' ', e.last_name) AS NVARCHAR(500)) AS manager_path,
            e.job_title AS department
        FROM staging.stg_employee e
        WHERE e.manager_id IS NULL

        UNION ALL

        -- Recursive: employees with managers
        SELECT
            e.business_entity_id AS employee_id,
            CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
            e.manager_id,
            h.employee_name AS manager_name,
            h.hierarchy_level + 1 AS hierarchy_level,
            CAST(h.manager_path + N' > ' + CONCAT(e.first_name, ' ', e.last_name) AS NVARCHAR(500)) AS manager_path,
            e.job_title AS department
        FROM staging.stg_employee e
        INNER JOIN hierarchy h
            ON e.manager_id = h.employee_id
    )
    INSERT INTO gold.rpt_employee_hierarchy (
        employee_id, employee_name, manager_id, manager_name,
        hierarchy_level, manager_path, department, direct_reports_count
    )
    SELECT
        hr.employee_id,
        hr.employee_name,
        hr.manager_id,
        hr.manager_name,
        hr.hierarchy_level,
        hr.manager_path,
        hr.department,
        (SELECT COUNT(*)
         FROM staging.stg_employee sub
         WHERE sub.manager_id = hr.employee_id) AS direct_reports_count
    FROM hierarchy hr;
END;
GO

-- ----------------------------------------------------------
-- #14 usp_load_rpt_sales_by_category
-- Target: gold.rpt_sales_by_category
-- Pattern: TRUNCATE+INSERT, GROUPING SETS, GROUPING()
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_sales_by_category
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_sales_by_category;

    INSERT INTO gold.rpt_sales_by_category (
        category_name, subcategory_name, date_key,
        total_revenue, total_qty, grouping_level
    )
    SELECT
        dp.product_category AS category_name,
        dp.product_subcategory AS subcategory_name,
        f.date_key,
        SUM(f.line_total) AS total_revenue,
        SUM(f.order_qty) AS total_qty,
        CASE
            WHEN GROUPING(dp.product_category) = 1 THEN 3  -- grand total
            WHEN GROUPING(dp.product_subcategory) = 1 AND GROUPING(f.date_key) = 1 THEN 2  -- category only
            WHEN GROUPING(f.date_key) = 1 THEN 1  -- category + subcategory
            ELSE 0  -- full detail
        END AS grouping_level
    FROM fact.fct_sales f
    INNER JOIN dim.dim_product dp
        ON f.product_key = dp.product_key
        AND dp.is_current = 1
    GROUP BY GROUPING SETS (
        (dp.product_category, dp.product_subcategory, f.date_key),
        (dp.product_category, f.date_key),
        (dp.product_category),
        ()
    );
END;
GO

-- ----------------------------------------------------------
-- #15 usp_load_rpt_channel_pivot
-- Target: gold.rpt_channel_pivot
-- Pattern: TRUNCATE+INSERT, native PIVOT, UNPIVOT validation
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_channel_pivot
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_channel_pivot;

    -- PIVOT: narrow fct_sales_by_channel → wide columns
    INSERT INTO gold.rpt_channel_pivot (
        date_key, online_revenue, store_revenue, online_qty, store_qty,
        online_order_count, store_order_count
    )
    SELECT
        pvt_rev.date_key,
        ISNULL(pvt_rev.[Online], 0) AS online_revenue,
        ISNULL(pvt_rev.[Store], 0) AS store_revenue,
        ISNULL(pvt_qty.online_qty, 0) AS online_qty,
        ISNULL(pvt_qty.store_qty, 0) AS store_qty,
        ISNULL(pvt_cnt.online_order_count, 0) AS online_order_count,
        ISNULL(pvt_cnt.store_order_count, 0) AS store_order_count
    FROM (
        SELECT date_key, channel, total_revenue
        FROM fact.fct_sales_by_channel
    ) src
    PIVOT (
        SUM(total_revenue)
        FOR channel IN ([Online], [Store])
    ) pvt_rev
    LEFT JOIN (
        SELECT
            date_key,
            SUM(CASE WHEN channel = N'Online' THEN total_qty ELSE 0 END) AS online_qty,
            SUM(CASE WHEN channel = N'Store' THEN total_qty ELSE 0 END) AS store_qty
        FROM fact.fct_sales_by_channel
        GROUP BY date_key
    ) pvt_qty ON pvt_rev.date_key = pvt_qty.date_key
    LEFT JOIN (
        SELECT
            date_key,
            SUM(CASE WHEN channel = N'Online' THEN order_count ELSE 0 END) AS online_order_count,
            SUM(CASE WHEN channel = N'Store' THEN order_count ELSE 0 END) AS store_order_count
        FROM fact.fct_sales_by_channel
        GROUP BY date_key
    ) pvt_cnt ON pvt_rev.date_key = pvt_cnt.date_key;

    -- UNPIVOT validation: pivot result back to narrow and verify row count
    DECLARE @pivot_rows INT, @unpivot_rows INT;

    SELECT @pivot_rows = COUNT(*) FROM gold.rpt_channel_pivot;

    SELECT @unpivot_rows = COUNT(*)
    FROM (
        SELECT date_key, channel_metric, metric_value
        FROM (
            SELECT date_key, online_revenue, store_revenue
            FROM gold.rpt_channel_pivot
        ) p
        UNPIVOT (
            metric_value FOR channel_metric IN (online_revenue, store_revenue)
        ) AS unpvt
    ) validation;

    DECLARE @expected_rows INT = @pivot_rows * 2;

    IF @unpivot_rows <> @expected_rows
    BEGIN
        RAISERROR(N'UNPIVOT validation failed: expected %d rows, got %d', 16, 1, @expected_rows, @unpivot_rows);
    END;
END;
GO

-- ----------------------------------------------------------
-- #16 usp_load_rpt_returns_analysis
-- Target: gold.rpt_returns_analysis
-- Pattern: TRUNCATE+INSERT, LEFT JOIN, NOT EXISTS,
--          NOT IN (with NULL guard), IN subquery, HAVING
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_returns_analysis
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_returns_analysis;

    INSERT INTO gold.rpt_returns_analysis (
        product_key, product_name, date_key,
        sales_qty, return_qty, return_rate, top_return_reason
    )
    SELECT
        dp.product_key,
        dp.product_name,
        f.date_key,
        SUM(f.order_qty) AS sales_qty,
        ISNULL(SUM(r.return_qty), 0) AS return_qty,
        CASE
            WHEN SUM(f.order_qty) = 0 THEN 0
            ELSE CAST(ISNULL(SUM(r.return_qty), 0) * 1.0 / SUM(f.order_qty) AS DECIMAL(10,4))
        END AS return_rate,
        MAX(r.return_reason) AS top_return_reason
    FROM fact.fct_sales f
    INNER JOIN dim.dim_product dp
        ON f.product_key = dp.product_key
        AND dp.is_current = 1
    LEFT JOIN staging.stg_returns r
        ON f.sales_order_id = r.sales_order_id
        AND f.sales_order_detail_id = r.sales_order_detail_id
    WHERE
        -- NOT EXISTS: exclude never-returned products that have been discontinued
        NOT EXISTS (
            SELECT 1
            FROM staging.stg_product sp
            WHERE sp.product_id = dp.product_id
            AND sp.discontinued_date IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM staging.stg_returns sr
                INNER JOIN staging.stg_sales_order_detail sod
                    ON sr.sales_order_id = sod.sales_order_id
                    AND sr.sales_order_detail_id = sod.sales_order_detail_id
                WHERE sod.product_id = sp.product_id
            )
        )
        -- NOT IN with IS NOT NULL guard: exclude specific credit card types
        AND f.credit_card_key NOT IN (
            SELECT cc.credit_card_key
            FROM dim.dim_credit_card cc
            WHERE cc.card_type = N'ColonialVoice'
            AND cc.credit_card_key IS NOT NULL
        )
        -- IN subquery: only territories with significant volume
        AND f.sales_key IN (
            SELECT f2.sales_key
            FROM fact.fct_sales f2
            INNER JOIN staging.stg_sales_order_header h2
                ON f2.sales_order_id = h2.sales_order_id
            WHERE h2.territory_id IN (1, 4, 6, 7, 9, 10)
        )
    GROUP BY dp.product_key, dp.product_name, f.date_key
    HAVING SUM(f.order_qty) >= 5;
END;
GO

-- ----------------------------------------------------------
-- #17 usp_load_rpt_customer_segments
-- Target: gold.rpt_customer_segments
-- Pattern: TRUNCATE+INSERT, EXCEPT, INTERSECT, UNION
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_customer_segments
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_customer_segments;

    -- Segment 1: Active customers (all minus inactive via EXCEPT)
    INSERT INTO gold.rpt_customer_segments (customer_id, segment_name, total_revenue, total_orders)
    SELECT
        customer_id,
        N'Active' AS segment_name,
        total_revenue,
        total_orders
    FROM (
        SELECT
            c.customer_id,
            ISNULL(SUM(h.total_due), 0) AS total_revenue,
            COUNT(DISTINCT h.sales_order_id) AS total_orders
        FROM staging.stg_customer c
        LEFT JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
        GROUP BY c.customer_id

        EXCEPT

        -- Inactive: customers with zero orders
        SELECT
            c2.customer_id,
            CAST(0 AS DECIMAL(38,6)) AS total_revenue,
            0 AS total_orders
        FROM staging.stg_customer c2
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.stg_sales_order_header h2
            WHERE h2.customer_id = c2.customer_id
        )
    ) active_set;

    -- Segment 2: High-value (active INTERSECT high-revenue)
    INSERT INTO gold.rpt_customer_segments (customer_id, segment_name, total_revenue, total_orders)
    SELECT
        customer_id,
        N'High-Value' AS segment_name,
        total_revenue,
        total_orders
    FROM (
        -- Active customers
        SELECT c.customer_id, SUM(h.total_due) AS total_revenue, COUNT(DISTINCT h.sales_order_id) AS total_orders
        FROM staging.stg_customer c
        INNER JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
        GROUP BY c.customer_id

        INTERSECT

        -- High-revenue customers (above average)
        SELECT c.customer_id, SUM(h.total_due) AS total_revenue, COUNT(DISTINCT h.sales_order_id) AS total_orders
        FROM staging.stg_customer c
        INNER JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
        GROUP BY c.customer_id
        HAVING SUM(h.total_due) >= (SELECT AVG(total_due) * 2 FROM staging.stg_sales_order_header)
    ) high_value_set;

    -- Segment 3: At-risk (single order, old) — combined via UNION with above segments
    INSERT INTO gold.rpt_customer_segments (customer_id, segment_name, total_revenue, total_orders)
    SELECT customer_id, segment_name, total_revenue, total_orders
    FROM (
        SELECT
            c.customer_id,
            N'At-Risk' AS segment_name,
            SUM(h.total_due) AS total_revenue,
            COUNT(DISTINCT h.sales_order_id) AS total_orders
        FROM staging.stg_customer c
        INNER JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
        GROUP BY c.customer_id
        HAVING COUNT(DISTINCT h.sales_order_id) = 1

        UNION

        SELECT
            c.customer_id,
            N'At-Risk' AS segment_name,
            SUM(h.total_due) AS total_revenue,
            COUNT(DISTINCT h.sales_order_id) AS total_orders
        FROM staging.stg_customer c
        INNER JOIN staging.stg_sales_order_header h ON c.customer_id = h.customer_id
        GROUP BY c.customer_id
        HAVING MAX(h.order_date) < DATEADD(YEAR, -2, GETDATE())
    ) at_risk_set;
END;
GO

-- ----------------------------------------------------------
-- #18 usp_load_rpt_address_coverage
-- Target: gold.rpt_address_coverage
-- Pattern: TRUNCATE+INSERT, FULL OUTER JOIN, COALESCE, CASE
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_rpt_address_coverage
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.rpt_address_coverage;

    INSERT INTO gold.rpt_address_coverage (
        address_id, staging_city, dim_city,
        staging_postal_code, dim_postal_code, coverage_status
    )
    SELECT
        COALESCE(sa.address_id, da.address_id) AS address_id,
        sa.city AS staging_city,
        da.city AS dim_city,
        sa.postal_code AS staging_postal_code,
        da.postal_code AS dim_postal_code,
        CASE
            WHEN da.address_id IS NULL THEN N'new'
            WHEN sa.address_id IS NULL THEN N'orphan'
            ELSE N'matched'
        END AS coverage_status
    FROM staging.stg_address sa
    FULL OUTER JOIN dim.dim_address da
        ON sa.address_id = da.address_id
        AND da.is_current = 1;
END;
GO

-- ----------------------------------------------------------
-- #19 usp_load_gold_agg_batch
-- Target: gold.rpt_product_margin + gold.rpt_date_sales_rollup
-- Pattern: Multi-table via EXEC, CUBE, ROLLUP, dynamic SQL
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_load_gold_agg_batch
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================
    -- Table 1: rpt_product_margin (CUBE aggregation)
    -- ==============================
    TRUNCATE TABLE gold.rpt_product_margin;

    INSERT INTO gold.rpt_product_margin (
        product_line, product_category, color,
        total_revenue, total_cost, total_margin, margin_pct, grouping_level
    )
    SELECT
        dp.product_line,
        dp.product_category,
        dp.color,
        SUM(f.line_total) AS total_revenue,
        SUM(f.order_qty * dp.standard_cost) AS total_cost,
        SUM(f.line_total) - SUM(f.order_qty * dp.standard_cost) AS total_margin,
        CASE
            WHEN SUM(f.line_total) = 0 THEN 0
            ELSE CAST((SUM(f.line_total) - SUM(f.order_qty * dp.standard_cost)) * 100.0 / SUM(f.line_total) AS DECIMAL(10,4))
        END AS margin_pct,
        GROUPING(dp.product_line) + GROUPING(dp.product_category) + GROUPING(dp.color) AS grouping_level
    FROM fact.fct_sales f
    INNER JOIN dim.dim_product dp
        ON f.product_key = dp.product_key
        AND dp.is_current = 1
    GROUP BY CUBE (dp.product_line, dp.product_category, dp.color);

    -- ==============================
    -- Table 2: rpt_date_sales_rollup (ROLLUP via dynamic SQL / EXEC)
    -- ==============================
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'
        TRUNCATE TABLE gold.rpt_date_sales_rollup;

        INSERT INTO gold.rpt_date_sales_rollup (
            year_number, quarter_number, month_number,
            total_revenue, total_qty, order_count, rollup_level
        )
        SELECT
            dd.year_number,
            dd.quarter_number,
            dd.month_number,
            SUM(f.line_total) AS total_revenue,
            SUM(f.order_qty) AS total_qty,
            COUNT(DISTINCT f.sales_order_id) AS order_count,
            GROUPING(dd.year_number) + GROUPING(dd.quarter_number) + GROUPING(dd.month_number) AS rollup_level
        FROM fact.fct_sales f
        INNER JOIN dim.dim_date dd
            ON f.date_key = dd.date_key
        GROUP BY ROLLUP (dd.year_number, dd.quarter_number, dd.month_number);
    ';

    EXEC sp_executesql @sql;
END;
GO

-- ==========================================================
-- Category: Exec orchestrator (#20)
-- ==========================================================

-- ----------------------------------------------------------
-- #20 usp_exec_orchestrator_full_load
-- Target: no direct table write
-- Pattern: Static EXEC, EXEC with params, EXEC with return value,
--          dynamic EXEC via sp_executesql
-- ----------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_exec_orchestrator_full_load
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @step NVARCHAR(100);
    DECLARE @rows INT;
    DECLARE @sql NVARCHAR(200);

    -- Step 1: Load dim_customer (static EXEC)
    SET @step = N'dim_customer';
    EXEC dbo.usp_load_dim_customer;

    -- Step 2: Load dim_employee (EXEC with params)
    SET @step = N'dim_employee';
    EXEC dbo.usp_load_dim_employee @mode = N'SCD1';

    -- Step 3: Load fct_sales_daily (EXEC with return value)
    SET @step = N'fct_sales_daily';
    EXEC @rows = dbo.usp_load_fct_sales_daily @mode = N'FULL';

    -- Step 4: Load fct_sales_summary (direct EXEC)
    SET @step = N'fct_sales_summary';
    EXEC dbo.usp_load_fct_sales_summary;

    -- Step 5: Load gold_agg_batch (dynamic EXEC via sp_executesql)
    SET @step = N'gold_agg_batch';
    SET @sql = N'EXEC dbo.usp_load_gold_agg_batch';
    EXEC sp_executesql @sql;
END;
GO
