-- ============================================================
-- SCENARIO: multi-table-writer — single proc targets two SCD2 dims
-- ============================================================
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
