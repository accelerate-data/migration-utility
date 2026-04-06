# Schema DDL

Dialect-specific DDL scripts for the Kimball DW fixture. Each script is self-contained and idempotent (drops and recreates all objects).

## Files

| File | Dialect | Target |
|---|---|---|
| `sqlserver.sql` | T-SQL | `KimballFixture` database, schemas: `staging`, `dim`, `fact` |
| `oracle.sql` | Oracle | `kimball` user in FREEPDB1 (flat namespace, no schema prefixes) |
| `postgres.sql` | PostgreSQL | `kimball_fixture` database, schemas: `staging`, `dim`, `fact` |

## Objects created

- 11 staging tables (`stg_customer`, `stg_person`, `stg_product`, `stg_product_subcategory`, `stg_product_category`, `stg_sales_order_header`, `stg_sales_order_detail`, `stg_address`, `stg_credit_card`, `stg_employee`, `stg_returns`)
- 8 dimension tables (`dim_customer`, `dim_product`, `dim_date`, `dim_employee`, `dim_product_category`, `dim_address`, `dim_credit_card`, `dim_order_status`)
- 3 fact tables (`fct_sales`, `fct_sales_summary`, `fct_sales_by_channel`)
- 4 views (`vw_stg_customer`, `vw_stg_product`, `vw_stg_sales`, `vw_sales_summary`)

## Validation

DDL was validated by running each script against its respective Docker engine on 2026-04-07:

- **SQL Server:** `docker exec aw-sql sqlcmd -d KimballFixture -i /tmp/sqlserver.sql` — all objects created, no errors
- **Oracle:** `sqlplus kimball/kimball@localhost:1521/FREEPDB1 @/tmp/oracle.sql` — all objects created, no errors
- **PostgreSQL:** `psql -U postgres -d kimball_fixture -f /tmp/postgres.sql` — all objects created, no errors

Baseline seed data and all 5 delta scenarios were also loaded successfully against all three engines.

## Design notes

- `dim_address` intentionally excludes `address_line_2` from `stg_address` — city and postal code are sufficient for SCD2 change detection in the fixture scenarios.
- Oracle uses `GENERATED ALWAYS AS IDENTITY` (12c+) for surrogate keys instead of sequences+triggers.
- `product_size` column (not `size`) is used in `stg_product` across all dialects because `SIZE` is an Oracle reserved word.
