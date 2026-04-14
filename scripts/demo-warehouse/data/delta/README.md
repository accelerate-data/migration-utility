# Delta Scenarios

Each subdirectory contains one incremental scenario with dialect-specific SQL files (`sqlserver.sql`, `oracle.sql`, `postgres.sql`). Run a delta against a database that already has the baseline loaded.

## Reset between scenarios

Each scenario must be applied to a **freshly loaded baseline**, not cumulatively on top of previous deltas. Scenario 05 permanently drops a PK constraint, and scenarios 01–04 insert rows with hard-coded IDs (99901, 99902) that would collide if re-run. To reset: re-run the schema DDL (`scripts/demo-warehouse/schema/<dialect>.sql`) followed by the baseline seed (`scripts/demo-warehouse/data/baseline/<dialect>.sql`).

## Scenarios

### 01-new-customer-product

New customer and product that don't exist in baseline. Exercises the INSERT-only path in dimension load procedures.

| Staging table | Rows added |
|---|---|
| `stg_person` | 1 (business_entity_id=99901) |
| `stg_customer` | 1 (customer_id=99901) |
| `stg_product` | 1 (product_id=99901) |

**Expected after procedure:** new `dim_customer` and `dim_product` rows with `is_current=1`.

### 02-scd2-address-change

Existing address reappears in staging with a different city. Exercises SCD Type 2 (expire + insert).

| Staging table | Change |
|---|---|
| `stg_address` | address_id=1 city changed to "New Delta City" |

**Expected after procedure:** `dim_address` has 2 rows for address_id=1 — one expired (`is_current=0`, `valid_to` set), one current.

### 03-scd1-name-correction

Existing person reappears with a corrected last name. Exercises SCD Type 1 (in-place overwrite).

| Staging table | Change |
|---|---|
| `stg_person` | business_entity_id=1 last_name changed to "Sanchez-Corrected" |

**Expected after procedure:** `dim_customer` row for person_id=1 still has exactly 1 row, `full_name` updated.

### 04-late-arriving-fact

A new sales order arrives with a historical order date (2011-06-15). Exercises late-arriving fact handling — the procedure must resolve dimension keys from historical dim rows, not just current ones.

| Staging table | Rows added |
|---|---|
| `stg_sales_order_header` | 1 (sales_order_id=99901, order_date=2011-06-15) |
| `stg_sales_order_detail` | 1 (detail_id=1, product_id=1) |

**Expected after procedure:** new `fct_sales` row with `date_key=20110615`.

### 05-dedup-staging

Duplicate rows in staging for the same order detail. Exercises MERGE dedup — the procedure must collapse duplicates before inserting into the fact table.

| Staging table | Rows added |
|---|---|
| `stg_sales_order_header` | 1 (sales_order_id=99902) |
| `stg_sales_order_detail` | 2 identical rows (detail_id=1) after PK drop |

**Expected after procedure:** exactly 1 `fct_sales` row for (sales_order_id=99902, detail_id=1).

**Note:** This scenario drops the PK constraint on `stg_sales_order_detail` to allow the duplicate. The constraint is not restored — the procedure is expected to handle dedup in its query logic.
