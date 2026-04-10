# Routine Migration Reference -- Oracle PL/SQL

Concise extraction rules for converting DML statements in Oracle PL/SQL routines into pure SELECT statements, with worked examples showing both the extracted SELECT and the refactored CTE version. Source: [dbt Migrate from stored procedures](https://docs.getdbt.com/guides/migrate-from-stored-procedures).

## INSERT...SELECT

Extract the SELECT portion. The INSERT column list maps to SELECT aliases. Nearly identical to T-SQL; use Oracle quoting (`"schema"."table"` or unquoted lowercase).

### Extraction rule

```sql
-- Original
INSERT INTO silver.dim_customer (customer_id, full_name)
SELECT customer_id, first_name || ' ' || last_name
FROM bronze.customer_raw WHERE is_active = 1;

-- Extracted SELECT
SELECT customer_id, first_name || ' ' || last_name AS full_name
FROM bronze.customer_raw WHERE is_active = 1
```

Multiple INSERTs to the same target: combine with `UNION ALL`.

### Worked example

```sql
-- Original routine
CREATE OR REPLACE PROCEDURE silver.load_insert_select_target AS
BEGIN
    INSERT INTO silver.insert_select_target (product_alternate_key, english_product_name)
    SELECT TO_CHAR(product_id), product_name
    FROM bronze.product;
END;
/

-- Extracted SELECT (sub-agent A)
SELECT
    TO_CHAR(product_id) AS product_alternate_key,
    product_name AS english_product_name
FROM bronze.product

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM bronze.product
),
final AS (
    SELECT
        TO_CHAR(product_id) AS product_alternate_key,
        product_name AS english_product_name
    FROM source_product
)
SELECT * FROM final
```

## MERGE

Extract the USING clause. Oracle uses parentheses around the ON condition: `MERGE INTO t USING s ON (t.id = s.id)`. No semicolon after each WHEN clause.

### Extraction rule

```sql
-- Original
MERGE INTO silver.dim_customer tgt
USING (
    SELECT c.customer_id, c.first_name, g.country
    FROM bronze.customer_raw c
    JOIN bronze.geography g ON c.geo_key = g.geo_key
) src ON (tgt.customer_id = src.customer_id)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Extracted SELECT (the USING clause)
SELECT c.customer_id, c.first_name, g.country
FROM bronze.customer_raw c
JOIN bronze.geography g ON c.geo_key = g.geo_key
```

If the MERGE has both MATCHED (UPDATE) and NOT MATCHED (INSERT) with different column sets, extract the USING clause as-is. The materialization strategy (incremental with merge) handles the write semantics downstream.

### Worked example

```sql
-- Original routine
CREATE OR REPLACE PROCEDURE silver.load_dim_product AS
BEGIN
    MERGE INTO silver.dim_product tgt
    USING (
        SELECT
            TO_CHAR(product_id) AS product_alternate_key,
            product_name AS english_product_name,
            standard_cost, list_price,
            NVL(color, '') AS color,
            CASE WHEN discontinued_date IS NOT NULL THEN 'Obsolete'
                 WHEN sell_end_date IS NOT NULL THEN 'Outdated'
                 ELSE 'Current' END AS status
        FROM bronze.product
    ) src ON (tgt.product_alternate_key = src.product_alternate_key)
    WHEN MATCHED THEN UPDATE SET
        tgt.english_product_name = src.english_product_name,
        tgt.standard_cost = src.standard_cost,
        tgt.list_price = src.list_price,
        tgt.color = src.color,
        tgt.status = src.status
    WHEN NOT MATCHED THEN INSERT (
        product_alternate_key, english_product_name,
        standard_cost, list_price, color, status)
    VALUES (
        src.product_alternate_key, src.english_product_name,
        src.standard_cost, src.list_price, src.color, src.status);
END;
/

-- Extracted SELECT (sub-agent A)
SELECT
    TO_CHAR(product_id) AS product_alternate_key,
    product_name AS english_product_name,
    standard_cost, list_price,
    NVL(color, '') AS color,
    CASE WHEN discontinued_date IS NOT NULL THEN 'Obsolete'
         WHEN sell_end_date IS NOT NULL THEN 'Outdated'
         ELSE 'Current' END AS status
FROM bronze.product

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM bronze.product
),
transformed_product AS (
    SELECT
        TO_CHAR(product_id) AS product_alternate_key,
        product_name AS english_product_name,
        standard_cost, list_price,
        NVL(color, '') AS color,
        CASE WHEN discontinued_date IS NOT NULL THEN 'Obsolete'
             WHEN sell_end_date IS NOT NULL THEN 'Outdated'
             ELSE 'Current' END AS status
    FROM source_product
),
final AS (
    SELECT * FROM transformed_product
)
SELECT * FROM final
```

## UPDATE

Oracle has NO `UPDATE...FROM...JOIN` syntax. Updates with joins use one of three patterns:

- **Correlated subquery:** `UPDATE t SET col = (SELECT val FROM s WHERE s.id = t.id)`
- **MERGE as UPDATE:** `MERGE INTO t USING s ON (...) WHEN MATCHED THEN UPDATE SET ...`
- **Inline view update** (if key-preserved): `UPDATE (SELECT t.col, s.val FROM t JOIN s ON ...) SET col = val`

The SET clause becomes CASE expressions or direct column references from the joined source. Include ALL columns from the target table, not just the updated ones.

### Extraction rule

Simple UPDATE with WHERE:

```sql
-- Original
UPDATE silver.orders
SET order_type = 'return'
WHERE total < 0;

-- Extracted SELECT: every target column, CASE for updated ones
SELECT
    order_id,
    CASE WHEN total < 0 THEN 'return' ELSE order_type END AS order_type,
    total,
    order_date
FROM silver.orders
```

UPDATE with correlated subquery (Oracle pattern for joined updates):

```sql
-- Original
UPDATE silver.orders tgt
SET tgt.region_name = (
    SELECT src.region_name
    FROM bronze.geography src
    WHERE src.geo_key = tgt.geo_key
);

-- Extracted SELECT: target columns, joined source for SET values
SELECT tgt.order_id, src.region_name, tgt.total, tgt.order_date
FROM silver.orders tgt
JOIN bronze.geography src ON tgt.geo_key = src.geo_key
```

### Worked example

```sql
-- Original routine (correlated subquery pattern)
CREATE OR REPLACE PROCEDURE silver.load_update_join_target AS
BEGIN
    UPDATE silver.update_join_target tgt
    SET
        tgt.english_product_name = (
            SELECT src.product_name
            FROM bronze.product src
            WHERE TO_CHAR(src.product_id) = tgt.product_alternate_key
        ),
        tgt.last_seen_date = SYSDATE
    WHERE EXISTS (
        SELECT 1 FROM bronze.product src
        WHERE TO_CHAR(src.product_id) = tgt.product_alternate_key
    );
END;
/

-- Target columns: product_alternate_key, english_product_name, last_seen_date

-- Extracted SELECT (sub-agent A)
-- All target columns. Updated columns use the SET source values.
-- Non-updated columns pass through from the target.
SELECT
    tgt.product_alternate_key,
    src.product_name AS english_product_name,
    SYSDATE AS last_seen_date
FROM silver.update_join_target tgt
JOIN bronze.product src
    ON TO_CHAR(src.product_id) = tgt.product_alternate_key

-- Refactored CTE (sub-agent B)
WITH source_product AS (
    SELECT * FROM bronze.product
),
existing_target AS (
    SELECT * FROM silver.update_join_target
),
updated AS (
    SELECT
        tgt.product_alternate_key,
        src.product_name AS english_product_name,
        SYSDATE AS last_seen_date
    FROM existing_target tgt
    JOIN source_product src
        ON TO_CHAR(src.product_id) = tgt.product_alternate_key
)
SELECT * FROM updated
```

## DELETE

Invert the WHERE clause to keep the rows that survive. The extracted SELECT returns the rows that would REMAIN after the DELETE.

### Extraction rule

```sql
-- Original
DELETE FROM silver.orders WHERE order_status IS NULL;

-- Extracted SELECT: invert condition, keep survivors
SELECT * FROM silver.orders WHERE order_status IS NOT NULL
```

For equality conditions, invert with `<>` and handle NULLs:

```sql
-- Original
DELETE FROM silver.target WHERE is_retired = 1;

-- Extracted SELECT: invert to keep non-retired, include NULLs
SELECT * FROM silver.target WHERE is_retired <> 1 OR is_retired IS NULL
```

### Worked example

```sql
-- Original routine
CREATE OR REPLACE PROCEDURE silver.load_delete_where_target AS
BEGIN
    DELETE FROM silver.delete_where_target
    WHERE is_retired = 1;
END;
/

-- Target columns: product_alternate_key, english_product_name, is_retired

-- Extracted SELECT (sub-agent A)
-- Invert the WHERE: keep rows where is_retired is NOT 1
-- Must handle NULL (IS NULL means not retired)
SELECT
    product_alternate_key,
    english_product_name,
    is_retired
FROM silver.delete_where_target
WHERE is_retired <> 1 OR is_retired IS NULL

-- Refactored CTE (sub-agent B)
WITH all_records AS (
    SELECT * FROM silver.delete_where_target
),
surviving AS (
    SELECT *
    FROM all_records
    WHERE is_retired <> 1 OR is_retired IS NULL
)
SELECT * FROM surviving
```

## TRUNCATE + INSERT (full reload)

TRUNCATE is a no-op for extraction -- it just means "replace all rows". In PL/SQL, DDL like TRUNCATE requires `EXECUTE IMMEDIATE`. Extract only the INSERT...SELECT portion; ignore the EXECUTE IMMEDIATE wrapper.

### Worked example

```sql
-- Original routine
CREATE OR REPLACE PROCEDURE silver.load_dim_customer_full AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.dim_customer';
    INSERT INTO silver.dim_customer (
        customer_alternate_key, first_name, middle_name, last_name, title,
        gender, marital_status, email_promotion, date_first_purchase)
    SELECT
        TO_CHAR(c.customer_id),
        p.first_name, p.middle_name, p.last_name, p.title,
        NULL AS gender, NULL AS marital_status, p.email_promotion,
        TRUNC(h.min_order_date) AS date_first_purchase
    FROM bronze.customer c
    JOIN bronze.person p ON c.person_id = p.business_entity_id
    LEFT JOIN (
        SELECT customer_id, MIN(order_date) AS min_order_date
        FROM bronze.sales_order_header
        GROUP BY customer_id
    ) h ON h.customer_id = c.customer_id;
END;
/

-- Extracted SELECT (sub-agent A)
SELECT
    TO_CHAR(c.customer_id) AS customer_alternate_key,
    p.first_name, p.middle_name, p.last_name, p.title,
    NULL AS gender, NULL AS marital_status, p.email_promotion,
    TRUNC(h.min_order_date) AS date_first_purchase
FROM bronze.customer c
JOIN bronze.person p ON c.person_id = p.business_entity_id
LEFT JOIN (
    SELECT customer_id, MIN(order_date) AS min_order_date
    FROM bronze.sales_order_header
    GROUP BY customer_id
) h ON h.customer_id = c.customer_id

-- Refactored CTE (sub-agent B)
WITH source_customer AS (
    SELECT * FROM bronze.customer
),
source_person AS (
    SELECT * FROM bronze.person
),
source_orders AS (
    SELECT * FROM bronze.sales_order_header
),
customer_first_purchase AS (
    SELECT customer_id, MIN(order_date) AS min_order_date
    FROM source_orders
    GROUP BY customer_id
),
final AS (
    SELECT
        TO_CHAR(c.customer_id) AS customer_alternate_key,
        p.first_name, p.middle_name, p.last_name, p.title,
        NULL AS gender, NULL AS marital_status, p.email_promotion,
        TRUNC(fp.min_order_date) AS date_first_purchase
    FROM source_customer c
    JOIN source_person p ON c.person_id = p.business_entity_id
    LEFT JOIN customer_first_purchase fp ON fp.customer_id = c.customer_id
)
SELECT * FROM final
```

## Cursor FOR Loops

Rewrite as set-based operations. Cursors that accumulate running totals become window functions. Oracle cursor FOR loops (`FOR rec IN (SELECT ...) LOOP`) are syntactic sugar but still row-by-row.

### Extraction rule

```sql
-- Original: cursor that computes running balance row by row
FOR rec IN (
    SELECT account_id, txn_date, amount
    FROM bronze.transactions
    ORDER BY txn_date
) LOOP
    v_balance := v_balance + rec.amount;
    INSERT INTO silver.balances VALUES (rec.account_id, rec.txn_date, v_balance);
END LOOP;

-- Extracted SELECT: use window function
SELECT
    account_id, txn_date, amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date) AS running_balance
FROM bronze.transactions

-- Refactored CTE
WITH source_transactions AS (
    SELECT * FROM bronze.transactions
),
with_running_balance AS (
    SELECT
        account_id, txn_date, amount,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date) AS running_balance
    FROM source_transactions
)
SELECT * FROM with_running_balance
```

## BULK COLLECT + FORALL

Oracle bulk operations use BULK COLLECT to load rows into a PL/SQL collection, then FORALL to DML in batch. Extract the SELECT from the BULK COLLECT and the target from the FORALL INSERT.

### Extraction rule

```sql
-- Original
DECLARE
    TYPE t_product_tab IS TABLE OF bronze.product%ROWTYPE;
    l_products t_product_tab;
BEGIN
    SELECT * BULK COLLECT INTO l_products
    FROM bronze.product
    WHERE list_price > 0;

    FORALL i IN 1 .. l_products.COUNT
        INSERT INTO silver.active_products VALUES l_products(i);
END;
/

-- Extracted SELECT: the BULK COLLECT query is the transformation
SELECT *
FROM bronze.product
WHERE list_price > 0

-- Refactored CTE
WITH source_product AS (
    SELECT * FROM bronze.product
),
final AS (
    SELECT *
    FROM source_product
    WHERE list_price > 0
)
SELECT * FROM final
```

## Dynamic SQL (EXECUTE IMMEDIATE)

Inline the constructed query. If the dynamic SQL is parameterized with USING, replace bind variables with their source column references or default values.

### Extraction rule

```sql
-- Original
EXECUTE IMMEDIATE
    'INSERT INTO silver.summary (category, total_amount)
     SELECT category, SUM(amount)
     FROM bronze.line_items
     WHERE region_id = :1
     GROUP BY category'
USING p_region_id;

-- Extracted SELECT: inline the query, replace bind variable
SELECT category, SUM(amount) AS total_amount
FROM bronze.line_items
WHERE region_id = p_region_id
GROUP BY category
```

## PIVOT

Oracle PIVOT syntax is preserved as-is in both extracted and refactored SQL.

### Worked example

```sql
-- Original routine
CREATE OR REPLACE PROCEDURE silver.load_territory_pivot AS
BEGIN
    INSERT INTO silver.territory_summary (territory_group, combined_sales)
    SELECT pvt.territory_group, pvt."1" + pvt."2" + pvt."3"
    FROM (
        SELECT territory_group, territory_id, sales_ytd
        FROM bronze.sales_territory
    )
    PIVOT (SUM(sales_ytd) FOR territory_id IN (1 AS "1", 2 AS "2", 3 AS "3")) pvt;
END;
/

-- Extracted SELECT (sub-agent A)
SELECT
    pvt.territory_group,
    pvt."1" + pvt."2" + pvt."3" AS combined_sales
FROM (
    SELECT territory_group, territory_id, sales_ytd
    FROM bronze.sales_territory
)
PIVOT (SUM(sales_ytd) FOR territory_id IN (1 AS "1", 2 AS "2", 3 AS "3")) pvt

-- Refactored CTE (sub-agent B)
WITH source_territory AS (
    SELECT territory_group, territory_id, sales_ytd
    FROM bronze.sales_territory
),
pivoted AS (
    SELECT
        pvt.territory_group,
        pvt."1" + pvt."2" + pvt."3" AS combined_sales
    FROM source_territory
    PIVOT (SUM(sales_ytd) FOR territory_id IN (1 AS "1", 2 AS "2", 3 AS "3")) pvt
)
SELECT * FROM pivoted
```

## Key Principles

1. The extracted SELECT must produce the same columns and rows as the original DML would write to the target table
2. Preserve all JOINs, WHERE clauses, GROUP BY, and HAVING exactly
3. Keep Oracle syntax (NVL, TO_CHAR, TO_DATE, TRUNC, SYSDATE, DECODE, etc.) -- dialect conversion happens later
4. Oracle has no `UPDATE...FROM` -- use correlated subquery or MERGE pattern
5. The extracted SELECT is the baseline for equivalence comparison
6. The refactored CTE must produce the same result as the extracted SELECT
7. Every source table gets its own import CTE
8. Each logical CTE does one thing: join, filter, aggregate, or transform
9. The final CTE or SELECT produces all target table columns
