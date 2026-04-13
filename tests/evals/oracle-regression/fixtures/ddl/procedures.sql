-- Oracle SH schema synthetic procedure for oracle-regression eval fixtures.
-- Source: scripts/sql/oracle/synthetic_sales_procedure.sql
--
-- Patterns covered by the Oracle regression fixture conventions:
--   Pattern  1 — INSERT...SELECT (INSERT INTO ... WITH ... SELECT)
--   Pattern  7 — MERGE INTO       (upsert via MERGE)
--   Pattern  9 — WITH clause      (CTE to aggregate)
--   Pattern 45 — IF/ELSIF         (volume-tier classification)
--   Pattern 46 — BEGIN...EXCEPTION (T-SQL TRY/CATCH PL/SQL equivalent)

CREATE OR REPLACE PROCEDURE SH.SUMMARIZE_CHANNEL_SALES AS
    v_tier  VARCHAR2(10);
BEGIN
    -- Pattern 46: BEGIN...EXCEPTION wraps the full body (T-SQL TRY/CATCH equivalent)

    -- Clear any previous run results so the procedure is idempotent.
    DELETE FROM CHANNEL_SALES_SUMMARY;

    -- Pattern 1 (INSERT...SELECT) + Pattern 9 (WITH / CTE):
    -- Aggregate total sales amount per channel and populate the summary table.
    INSERT INTO CHANNEL_SALES_SUMMARY
        (CHANNEL_ID, CHANNEL_DESC, TOTAL_AMOUNT, TIER, UPDATED_AT)
    WITH channel_totals AS (
        SELECT
            c.CHANNEL_ID,
            c.CHANNEL_DESC,
            SUM(s.AMOUNT_SOLD) AS TOTAL_AMOUNT
        FROM   CHANNELS c
        JOIN   SALES    s ON s.CHANNEL_ID = c.CHANNEL_ID
        GROUP BY c.CHANNEL_ID, c.CHANNEL_DESC
    )
    SELECT CHANNEL_ID, CHANNEL_DESC, TOTAL_AMOUNT, 'UNKNOWN', SYSDATE
    FROM   channel_totals;

    -- Pattern 45 (IF/ELSIF): classify each channel by sales volume.
    FOR rec IN (SELECT CHANNEL_ID, TOTAL_AMOUNT FROM CHANNEL_SALES_SUMMARY) LOOP
        IF rec.TOTAL_AMOUNT >= 500000 THEN
            v_tier := 'HIGH';
        ELSIF rec.TOTAL_AMOUNT >= 100000 THEN
            v_tier := 'MEDIUM';
        ELSE
            v_tier := 'LOW';
        END IF;

        -- Pattern 7 (MERGE INTO): upsert the computed tier back into summary.
        MERGE INTO CHANNEL_SALES_SUMMARY tgt
        USING (SELECT rec.CHANNEL_ID AS CHANNEL_ID, v_tier AS TIER FROM DUAL) src
        ON    (tgt.CHANNEL_ID = src.CHANNEL_ID)
        WHEN MATCHED THEN
            UPDATE SET tgt.TIER = src.TIER, tgt.UPDATED_AT = SYSDATE;
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END SUMMARIZE_CHANNEL_SALES;
/
