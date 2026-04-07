# Schema DDL

Dialect-specific DDL scripts for the Kimball DW fixture. Each script is self-contained and idempotent (drops and recreates all objects).

## Files

| File | Dialect | Target |
|---|---|---|
| `sqlserver.sql` | T-SQL | `KimballFixture` database, schemas: `staging`, `dim`, `fact`, `gold` |
| `oracle.sql` | Oracle | `kimball` user in FREEPDB1 (flat namespace, `rpt_*` prefix for gold) |
| `postgres.sql` | PostgreSQL | `kimball_fixture` database, schemas: `staging`, `dim`, `fact`, `gold` |

## Objects created

- 11 staging tables (`stg_customer`, `stg_person`, `stg_product`, `stg_product_subcategory`, `stg_product_category`, `stg_sales_order_header`, `stg_sales_order_detail`, `stg_address`, `stg_credit_card`, `stg_employee`, `stg_returns`)
- 8 dimension tables (`dim_customer`, `dim_product`, `dim_date`, `dim_employee`, `dim_product_category`, `dim_address`, `dim_credit_card`, `dim_order_status`)
- 3 fact tables (`fct_sales`, `fct_sales_summary`, `fct_sales_by_channel`)
- 11 gold/reporting tables (`rpt_customer_lifetime_value`, `rpt_product_performance`, `rpt_sales_by_territory`, `rpt_employee_hierarchy`, `rpt_sales_by_category`, `rpt_channel_pivot`, `rpt_returns_analysis`, `rpt_customer_segments`, `rpt_address_coverage`, `rpt_product_margin`, `rpt_date_sales_rollup`)
- 6 views (`vw_stg_customer`, `vw_stg_product`, `vw_stg_sales`, `vw_sales_summary`, `vw_enriched_sales`, `vw_customer_360`)

## Validation

DDL was validated by running each script against its respective Docker engine on 2026-04-07:

- **SQL Server:** `docker exec sql-test sqlcmd -d KimballFixture -i /tmp/sqlserver.sql` — all objects created, no errors
- **Oracle:** `sqlplus kimball/kimball@localhost:1521/FREEPDB1 @/tmp/oracle.sql` — all objects created, no errors
- **PostgreSQL:** `psql -U postgres -d kimball_fixture -f /tmp/postgres.sql` — all objects created, no errors

Baseline seed data and all 5 delta scenarios were also loaded successfully against all three engines.

## Design notes

- `dim_address` intentionally excludes `address_line_2` from `stg_address` — city and postal code are sufficient for SCD2 change detection in the fixture scenarios.
- Oracle uses `GENERATED ALWAYS AS IDENTITY` (12c+) for surrogate keys instead of sequences+triggers.
- `product_size` column (not `size`) is used in `stg_product` across all dialects because `SIZE` is an Oracle reserved word.
- `stg_employee.manager_id` is a self-referencing FK to `business_entity_id` for recursive CTE hierarchy testing (4-5 levels deep, CEO at root).
- Gold-layer tables use `gold` schema (SQL Server/PostgreSQL) or `rpt_` prefix (Oracle flat namespace).
