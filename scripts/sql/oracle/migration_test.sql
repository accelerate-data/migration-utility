WHENEVER SQLERROR EXIT SQL.SQLCODE

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TABLES
    WHERE OWNER = UPPER('__SCHEMA__') AND TABLE_NAME = 'CHANNELS';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE "__SCHEMA__"."CHANNELS" (
                "CHANNEL_ID" NUMBER NOT NULL,
                "CHANNEL_DESC" VARCHAR2(20) NOT NULL,
                "CHANNEL_CLASS" VARCHAR2(20),
                "CHANNEL_CLASS_ID" NUMBER,
                "CHANNEL_TOTAL" VARCHAR2(30),
                "CHANNEL_TOTAL_ID" NUMBER,
                CONSTRAINT "__SCHEMA___CHANNELS_PK" PRIMARY KEY ("CHANNEL_ID")
            )
        ]';
    END IF;
END;
/

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TABLES
    WHERE OWNER = UPPER('__SCHEMA__') AND TABLE_NAME = 'SALES';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE "__SCHEMA__"."SALES" (
                "PROD_ID" NUMBER NOT NULL,
                "CUST_ID" NUMBER NOT NULL,
                "TIME_ID" DATE NOT NULL,
                "CHANNEL_ID" NUMBER NOT NULL,
                "PROMO_ID" NUMBER,
                "QUANTITY_SOLD" NUMBER,
                "AMOUNT_SOLD" NUMBER,
                CONSTRAINT "__SCHEMA___SALES_CHANNELS_FK"
                    FOREIGN KEY ("CHANNEL_ID")
                    REFERENCES "__SCHEMA__"."CHANNELS" ("CHANNEL_ID")
            )
        ]';
    END IF;
END;
/

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TABLES
    WHERE OWNER = UPPER('__SCHEMA__') AND TABLE_NAME = 'CHANNEL_SALES_SUMMARY';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE "__SCHEMA__"."CHANNEL_SALES_SUMMARY" (
                "CHANNEL_ID" NUMBER NOT NULL,
                "CHANNEL_DESC" VARCHAR2(20) NOT NULL,
                "TOTAL_AMOUNT" NUMBER NOT NULL,
                "TIER" VARCHAR2(10) NOT NULL,
                "UPDATED_AT" DATE NOT NULL,
                CONSTRAINT "__SCHEMA___CHANNEL_SALES_SUMMARY_PK" PRIMARY KEY ("CHANNEL_ID")
            )
        ]';
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE "__SCHEMA__".SUMMARIZE_CHANNEL_SALES AS
    v_tier VARCHAR2(10);
BEGIN
    DELETE FROM CHANNEL_SALES_SUMMARY;

    INSERT INTO CHANNEL_SALES_SUMMARY
        ("CHANNEL_ID", "CHANNEL_DESC", "TOTAL_AMOUNT", "TIER", "UPDATED_AT")
    WITH channel_totals AS (
        SELECT
            c."CHANNEL_ID",
            c."CHANNEL_DESC",
            SUM(s."AMOUNT_SOLD") AS total_amount
        FROM CHANNELS c
        JOIN SALES s ON s."CHANNEL_ID" = c."CHANNEL_ID"
        GROUP BY c."CHANNEL_ID", c."CHANNEL_DESC"
    )
    SELECT "CHANNEL_ID", "CHANNEL_DESC", total_amount, 'UNKNOWN', SYSDATE
    FROM channel_totals;

    FOR rec IN (
        SELECT "CHANNEL_ID", "TOTAL_AMOUNT"
        FROM CHANNEL_SALES_SUMMARY
    ) LOOP
        IF rec."TOTAL_AMOUNT" >= 500000 THEN
            v_tier := 'HIGH';
        ELSIF rec."TOTAL_AMOUNT" >= 100000 THEN
            v_tier := 'MEDIUM';
        ELSE
            v_tier := 'LOW';
        END IF;

        MERGE INTO CHANNEL_SALES_SUMMARY tgt
        USING (SELECT rec."CHANNEL_ID" AS channel_id, v_tier AS tier FROM dual) src
        ON (tgt."CHANNEL_ID" = src.channel_id)
        WHEN MATCHED THEN
            UPDATE SET tgt."TIER" = src.tier, tgt."UPDATED_AT" = SYSDATE;
    END LOOP;
END SUMMARIZE_CHANNEL_SALES;
/

CREATE OR REPLACE VIEW "__SCHEMA__".V_CHANNEL_SALES AS
SELECT
    c."CHANNEL_ID",
    c."CHANNEL_DESC",
    NVL(SUM(s."AMOUNT_SOLD"), 0) AS "TOTAL_AMOUNT"
FROM "__SCHEMA__"."CHANNELS" c
LEFT JOIN "__SCHEMA__"."SALES" s
    ON s."CHANNEL_ID" = c."CHANNEL_ID"
GROUP BY c."CHANNEL_ID", c."CHANNEL_DESC";
/

COMMIT;
/
