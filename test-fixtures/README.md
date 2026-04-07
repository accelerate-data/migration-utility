# Test Fixtures

Cross-dialect Kimball DW fixture for integration testing and demos. All fixtures are frozen after initial creation — do not regenerate.

## Fixture Layout

| Directory | Contents |
|---|---|
| `schema/` | DDL for all tables and views (SQL Server, Oracle, PostgreSQL) |
| `data/baseline/` | Seed INSERT statements per dialect |
| `data/delta/` | 5 incremental scenarios per dialect |
| `procedures/` | Stored procedure fixtures per dialect (this document) |
| `scripts/` | Extraction and generation utilities |

## Schema Overview

### Schemas

| Schema | SQL Server | Oracle | PostgreSQL | Purpose |
|---|---|---|---|---|
| staging | `staging.*` | flat (no prefix) | `staging.*` | Raw extracted data |
| dim | `dim.*` | flat (no prefix) | `dim.*` | Conformed dimensions |
| fact | `fact.*` | flat (no prefix) | `fact.*` | Fact tables |
| gold | `gold.*` | `rpt_*` prefix | `gold.*` | Analytics/reporting layer |

### Tables

#### Staging (11 tables — source only, no load procs)

| Table | Rows | Notes |
|---|---|---|
| stg_customer | 5,000 | FK to person, store, territory |
| stg_person | 5,000 | Names, person type |
| stg_product | 504 | Cost, price, category refs |
| stg_product_subcategory | 37 | FK to product_category |
| stg_product_category | 4 | Bikes, Components, Clothing, Accessories |
| stg_sales_order_header | 5,000 | online_order_flag drives channel split |
| stg_sales_order_detail | 5,000 | Line items |
| stg_address | 5,000 | City, postal code |
| stg_credit_card | 5,000 | Card type, expiration |
| stg_employee | 290 | Includes manager_id for org hierarchy |
| stg_returns | 250 | 5% sample of order details |

#### Dimensions (8 tables — 6 loaded by procs, 2 static)

| Table | SCD Type | Load Proc | Notes |
|---|---|---|---|
| dim_customer | SCD2 | `usp_load_dim_customer` | valid_from/valid_to/is_current |
| dim_product | SCD2 | `usp_load_dim_product` | Denormalized with subcategory+category |
| dim_employee | SCD1 | `usp_load_dim_employee` | manager_id self-reference for hierarchy |
| dim_product_category | SCD2 | `usp_load_dim_product_category` | Simple reference dim |
| dim_address | SCD2 | `usp_load_dim_address_and_credit_card` | Multi-table proc (shared with dim_credit_card) |
| dim_credit_card | SCD2 | `usp_load_dim_address_and_credit_card` | Multi-table proc (shared with dim_address) |
| dim_date | Static | — | Date spine, no load proc |
| dim_order_status | Static | — | 6-row reference, no load proc |

#### Facts (3 tables)

| Table | Load Proc | Notes |
|---|---|---|
| fct_sales | `usp_load_fct_sales_daily` + `usp_load_fct_sales_historical` | **Multi-writer conflict** — daily incremental vs historical full rebuild |
| fct_sales_summary | `usp_load_fct_sales_summary` | Aggregated by date+product |
| fct_sales_by_channel | `usp_load_fct_sales_by_channel` | No baseline data — computed from staging |

#### Gold / Reporting (11 tables)

| Table | Load Proc | Purpose |
|---|---|---|
| rpt_customer_lifetime_value | `usp_load_rpt_customer_lifetime_value` | LTV: total orders, revenue, avg order, tier |
| rpt_product_performance | `usp_load_rpt_product_performance` | Monthly revenue, rank, MoM growth, trend |
| rpt_sales_by_territory | `usp_load_rpt_sales_by_territory` | Territory-level rollup with rank |
| rpt_employee_hierarchy | `usp_load_rpt_employee_hierarchy` | Flattened org chart from recursive CTE |
| rpt_sales_by_category | `usp_load_rpt_sales_by_category` | Multi-level category sales |
| rpt_channel_pivot | `usp_load_rpt_channel_pivot` | Wide-format channel sales |
| rpt_returns_analysis | `usp_load_rpt_returns_analysis` | Return rates and reasons by product |
| rpt_customer_segments | `usp_load_rpt_customer_segments` | Customer segmentation via set operations |
| rpt_address_coverage | `usp_load_rpt_address_coverage` | Staging vs dim gap analysis |
| rpt_product_margin | `usp_load_gold_agg_batch` | Margin by line/category/color (CUBE) |
| rpt_date_sales_rollup | `usp_load_gold_agg_batch` | Date hierarchy rollup (ROLLUP) |

### Views (6 total — 4 existing + 2 new)

| View | Schema | Complexity | Used By |
|---|---|---|---|
| vw_stg_customer | staging | LEFT JOIN person for full_name | `usp_load_dim_customer` |
| vw_stg_product | staging | CASE for is_active, CAST | `usp_load_dim_product` |
| vw_stg_sales | staging | LEFT JOIN returns for is_returned | `usp_load_fct_sales_daily` |
| vw_sales_summary | staging | INNER JOIN + GROUP BY aggregate | `usp_load_fct_sales_summary` |
| vw_enriched_sales | staging | **New** — multi-join + ROW_NUMBER + LAG + CASE trend | `usp_load_rpt_product_performance` |
| vw_customer_360 | staging | **New** — CTE + aggregate subquery + CASE tier + LEFT JOINs | `usp_load_rpt_customer_lifetime_value` |

## Stored Procedure Inventory

### Summary: 20 target tables → 21 procedures

| Category | Tables | Procs | Notes |
|---|---|---|---|
| Dimension (individual) | 4 | 4 | 1 proc per dim |
| Dimension (multi-table MERGE) | 2 | 1 | dim_address + dim_credit_card |
| Fact (individual) | 3 | 3 | 1 proc per fact |
| Fact (multi-writer conflict) | — | +1 | fct_sales has daily + historical |
| Gold (individual) | 9 | 9 | 1 proc per gold table |
| Gold (multi-table EXEC) | 2 | 1 | rpt_product_margin + rpt_date_sales_rollup |
| Exec orchestrator | — | 1 | Calls other procs, no direct table write |
| **Total** | **20** | **21** | |

### Procedure Detail

#### Dimension Procs

**#1 `usp_load_dim_customer`** → dim_customer

- MERGE SCD2 (expire old + insert new on attribute change)
- Single CTE from vw_stg_customer (view-backed)
- LEFT JOIN stg_person for name enrichment
- CASE WHEN for full_name assembly
- Contains EXEC call to a helper for address enrichment (exec pattern woven into dim build)

**#2 `usp_load_dim_product`** → dim_product

- MERGE SCD2 with CTE source
- Multi-level CTE: stg_product → join stg_product_subcategory → join stg_product_category
- INNER JOINs across 3 staging tables
- PG: uses INSERT...ON CONFLICT for one of the upsert paths (mixed with MERGE)

**#3 `usp_load_dim_employee`** → dim_employee

- MERGE SCD1 (overwrite on match, insert on no match)
- Self-join on manager_id for manager_name lookup
- COALESCE for nullable manager fields
- Scalar subquery for direct_reports_count

**#4 `usp_load_dim_product_category`** → dim_product_category

- MERGE SCD2 from staging
- EXISTS check for change detection

**#5 `usp_load_dim_address_and_credit_card`** → dim_address + dim_credit_card

- **Multi-table: two MERGE statements in one proc**
- First MERGE: SCD2 on dim_address from stg_address
- Second MERGE: SCD2 on dim_credit_card from stg_credit_card
- OUTER APPLY / LEFT JOIN LATERAL to get latest credit card per address (cross-entity correlation)

#### Fact Procs

**#6 `usp_load_fct_sales_daily`** → fct_sales (incremental)

- INSERT...SELECT with multi-table INNER JOIN (header + detail + all 6 dimension lookups)
- EXISTS filter: only rows not already loaded
- Derived table for average price calculation
- Correlated subquery for discount validation
- IF/ELSE: @mode parameter ('FULL' vs 'INCREMENTAL')
- TRY/CATCH wrapping the main INSERT
- CROSS APPLY / LATERAL for top-N order detail enrichment

**#7 `usp_load_fct_sales_historical`** → fct_sales (full rebuild) — **CONFLICT with #6**

- TRUNCATE + INSERT (no incremental filter)
- Same dimension joins as #6 but intentionally ugly — all inline, no CTEs, messy formatting
- WHILE batch loop: loads in chunks of 1000 rows
- Nested control flow: IF (row_count > threshold) THEN WHILE batch ELSE single-pass, wrapped in TRY/CATCH

**#8 `usp_load_fct_sales_summary`** → fct_sales_summary

- TRUNCATE + INSERT
- GROUP BY aggregate from fct_sales joined to dim_date
- ROLLUP woven in: (year, quarter, month) hierarchy with GROUPING() function for level detection

**#9 `usp_load_fct_sales_by_channel`** → fct_sales_by_channel

- TRUNCATE + INSERT
- CTE with UNION ALL: online orders (online_order_flag = 1) UNION ALL store orders (online_order_flag = 0)
- Aggregation by date_key + channel

#### Gold-Layer Procs

**#10 `usp_load_rpt_customer_lifetime_value`** → rpt_customer_lifetime_value

- TRUNCATE + INSERT
- **View-backed**: reads from vw_customer_360 (complex view with CTE + aggregate subquery + CASE tier)
- Window functions: RANK for revenue ranking, NTILE(4) for tier classification
- Scalar subquery for avg order value

**#11 `usp_load_rpt_product_performance`** → rpt_product_performance

- TRUNCATE + INSERT
- **View-backed**: reads from vw_enriched_sales (complex view with ROW_NUMBER + LAG + CASE)
- Multi-level CTE: monthly aggregate → LAG for MoM growth → RANK for ranking
- Window functions: LAG, RANK, CASE for trend classification

**#12 `usp_load_rpt_sales_by_territory`** → rpt_sales_by_territory

- TRUNCATE + INSERT
- CROSS JOIN scaffold: distinct territories × distinct date_keys (generates all combos)
- LEFT JOIN actual sales aggregates onto scaffold (zero-fill gaps)
- RIGHT JOIN variant for territory coverage validation
- RANK window function for territory ranking

**#13 `usp_load_rpt_employee_hierarchy`** → rpt_employee_hierarchy

- TRUNCATE + INSERT
- **Recursive CTE**: anchor = employees WHERE manager_id IS NULL, recursive = join on manager_id = business_entity_id
- Computes hierarchy_level and manager_path (string aggregation)
- Scalar subquery for direct_reports_count per employee
- Oracle: recursive WITH (12c+). PG: WITH RECURSIVE.

**#14 `usp_load_rpt_sales_by_category`** → rpt_sales_by_category

- TRUNCATE + INSERT
- **GROUPING SETS**: (category_name, subcategory_name, date_key), (category_name, date_key), (category_name), ()
- GROUPING() function for level identification
- Joins fct_sales → dim_product → category hierarchy

**#15 `usp_load_rpt_channel_pivot`** → rpt_channel_pivot

- TRUNCATE + INSERT
- **PIVOT**: narrow fct_sales_by_channel → wide columns (online_revenue, store_revenue, online_qty, store_qty)
- SQL Server/Oracle: native PIVOT syntax
- PostgreSQL: conditional aggregation (SUM(CASE WHEN channel = 'Online' THEN ...))
- **UNPIVOT** validation step: unpivots result back to narrow and compares row count (exercises both patterns)
- SQL Server/Oracle: native UNPIVOT. PG: VALUES lateral.

**#16 `usp_load_rpt_returns_analysis`** → rpt_returns_analysis

- TRUNCATE + INSERT
- LEFT JOIN fct_sales to stg_returns for return matching
- **NOT EXISTS** for identifying never-returned products
- **NOT IN** with NULL guard for credit card exclusion list
- **IN** subquery for territory filter
- Aggregate + HAVING for minimum sales threshold

**#17 `usp_load_rpt_customer_segments`** → rpt_customer_segments

- TRUNCATE + INSERT
- **EXCEPT / MINUS** (Oracle): all customers minus inactive → active set
- **INTERSECT**: active customers ∩ high-value customers (by revenue threshold)
- **UNION**: combines computed segments (active, high-value, at-risk) into one result

**#18 `usp_load_rpt_address_coverage`** → rpt_address_coverage

- TRUNCATE + INSERT
- **FULL OUTER JOIN**: stg_address vs dim_address on address_id
- COALESCE for unified columns from either side
- CASE for gap classification: 'new' (in staging only), 'orphan' (in dim only), 'matched'

**#19 `usp_load_gold_agg_batch`** → rpt_product_margin + rpt_date_sales_rollup

- **Multi-table via EXEC**: one proc writes to 2 gold tables
- Uses EXEC / EXECUTE IMMEDIATE to run:
  - **CUBE** aggregation: margin analysis by (product_line, category, color) → rpt_product_margin
  - **ROLLUP** aggregation: date hierarchy (year, quarter, month) → rpt_date_sales_rollup
- SQL Server: EXEC with inline SQL string. Oracle: EXECUTE IMMEDIATE. PG: EXECUTE.

#### Exec Orchestrator

**#20 `usp_exec_orchestrator_full_load`** → no direct table write

- Calls procs in dependency order:
  1. `usp_load_dim_customer` — via direct EXEC / direct call
  2. `usp_load_dim_employee` — via EXEC with params (@mode = 'SCD1') / EXECUTE IMMEDIATE
  3. `usp_load_fct_sales_daily` — via EXEC with OUTPUT param (row count) / OUT param
  4. `usp_load_fct_sales_summary` — via EXEC with return value / function return
  5. `usp_load_gold_agg_batch` — via EXEC string / EXECUTE IMMEDIATE
- Demonstrates all EXEC variants: static, with params, with OUTPUT, with return value, dynamic string

## Pattern Coverage Cross-Reference

All 11 AC categories covered:

| # | Category | Patterns | Procs |
|---|---|---|---|
| 1 | Standard load | INSERT...SELECT, TRUNCATE+INSERT, MERGE SCD1, MERGE SCD2, MERGE+CTE | #2, #3, #4, #5, #6, #7, #8+ |
| 2 | CTE | Single, multi-level, UNION ALL in CTE, recursive | #1, #2, #9, #11, #13 |
| 3 | Join | INNER, LEFT, RIGHT, FULL OUTER, SELF, CROSS, CROSS APPLY/LATERAL, OUTER APPLY/LATERAL, derived table, correlated subquery | #1, #2, #3, #5, #6, #12, #17, #18 |
| 4 | Set operations | UNION ALL, UNION, EXCEPT/MINUS, INTERSECT | #9, #17 |
| 5 | Subquery | Scalar, EXISTS, NOT EXISTS, IN, NOT IN, WHERE | #3, #4, #6, #10, #13, #16 |
| 6 | Aggregate | GROUPING SETS, CUBE, ROLLUP | #8, #14, #19 |
| 7 | Pivot | PIVOT, UNPIVOT | #15 |
| 8 | Control flow | IF/ELSE, TRY/CATCH, WHILE, nested | #6, #7 |
| 9 | Exec chains | Static, params, OUTPUT, return value, EXECUTE IMMEDIATE | #1, #19, #20 |
| 10 | View-backed | Procs reading from complex views | #1, #10, #11 |
| 11 | Coverage matrix | This document | — |

## Error Conditions for Utility Testing

| Scenario | Tables | Procs | What Utility Must Do |
|---|---|---|---|
| Multi-writer conflict | fct_sales | #6 (daily) vs #7 (historical) | Choose one proc as the canonical loader |
| Multi-table MERGE | dim_address + dim_credit_card | #5 | Detect proc writes to 2 tables, assign ownership |
| Multi-table EXEC | rpt_product_margin + rpt_date_sales_rollup | #19 | Detect dynamic SQL writes to 2 tables, assign ownership |

## Dialect-Specific Notes

| Aspect | SQL Server | Oracle | PostgreSQL |
|---|---|---|---|
| Gold schema | `gold.rpt_*` | `rpt_*` (flat namespace) | `gold.rpt_*` |
| Proc CREATE | `CREATE OR ALTER PROCEDURE` | `CREATE OR REPLACE PROCEDURE` | `CREATE OR REPLACE PROCEDURE ... LANGUAGE plpgsql AS $$` |
| APPLY | `CROSS APPLY` / `OUTER APPLY` | `CROSS JOIN LATERAL` / `LEFT JOIN LATERAL` | `CROSS JOIN LATERAL` / `LEFT JOIN LATERAL ... ON TRUE` |
| EXCEPT | `EXCEPT` | `MINUS` | `EXCEPT` |
| PIVOT | Native `PIVOT` | Native `PIVOT` | Conditional aggregation |
| UNPIVOT | Native `UNPIVOT` | Native `UNPIVOT` | `VALUES` lateral |
| Error handling | `BEGIN TRY/CATCH` | `EXCEPTION WHEN OTHERS` | Nested `BEGIN ... EXCEPTION ... END` |
| EXEC | `EXEC dbo.usp_*` | Direct call / `EXECUTE IMMEDIATE` | `CALL` / `EXECUTE` |
| TRUNCATE in proc | Direct | `EXECUTE IMMEDIATE 'TRUNCATE TABLE ...'` | Direct |
| Recursive CTE | `WITH cte AS` | `WITH cte AS` (12c+) | `WITH RECURSIVE cte AS` |
| MERGE | `MERGE` | `MERGE` | Mix `MERGE` (PG15+) and `INSERT...ON CONFLICT` |
| Booleans | `BIT` (0/1) | `NUMBER(1)` (0/1) | `BOOLEAN` |
| String concat | `CONCAT()` or `+` | `\|\|` | `\|\|` |
| Date key | `CAST(CONVERT(VARCHAR(8), d, 112) AS INT)` | `TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))` | `CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)` |

## Install Order

Each dialect's procedures file follows this order:

1. Drop all procedures (reverse dependency order, idempotent)
2. Drop + create new complex views (vw_enriched_sales, vw_customer_360)
3. Dimension load procs (#1-5) — leaf procs, no proc dependencies
4. Fact load procs (#6-9)
5. Gold-layer procs (#10-18)
6. Exec orchestrator (#20) — must come last (calls procs from all categories)

**Data dependency:** #15 (rpt_channel_pivot) requires fct_sales_by_channel to be populated. Run #9 first at execution time.

## Validation

Manual Docker validation per dialect:

```bash
# SQL Server
docker cp test-fixtures/schema/sqlserver.sql sql-test:/tmp/
docker exec sql-test sqlcmd -d KimballFixture -i /tmp/sqlserver.sql
docker cp test-fixtures/procedures/sqlserver.sql sql-test:/tmp/
docker exec sql-test sqlcmd -d KimballFixture -i /tmp/procedures.sql

# Oracle
sqlplus kimball/kimball@localhost:1521/FREEPDB1 @test-fixtures/schema/oracle.sql
sqlplus kimball/kimball@localhost:1521/FREEPDB1 @test-fixtures/procedures/oracle.sql

# PostgreSQL
psql -U postgres -d kimball_fixture -f test-fixtures/schema/postgres.sql
psql -U postgres -d kimball_fixture -f test-fixtures/procedures/postgres.sql
```

Cross-dialect output parity validated via `test-fixtures/parity/validate.py`.
