# SQL Server Source Rules

Applies when the source system is SQL Server or Azure SQL. Load strategy, migration flags, and
output discipline here supplement the shared conventions in `CLAUDE.md`.

## T-SQL Pattern Recognition

Map each stored procedure pattern to its dbt equivalent before classifying or generating output.

| Pattern | Migration approach |
|---|---|
| `TRUNCATE TABLE / INSERT` | `full_refresh` materialization |
| `INSERT INTO ... SELECT` with no preceding delete | `incremental` (append) |
| `MERGE ... USING ... WHEN MATCHED` | Flag as **needs review** — prefer `delete+insert` or `full_refresh` |
| `#temp_table` / `##global_temp` | Replace with CTEs or ephemeral dbt models |
| Dynamic SQL / `sp_executesql` | Flag for manual review — cannot auto-migrate |
| Cursor / row-by-row processing | Flag for manual review — cannot auto-migrate |
| `IDENTITY` columns | Use `dbt_utils.generate_surrogate_key`; do not carry IDENTITY forward |
| `datetime2` columns in snapshot context | Cast to `date` or `timestamp_ntz` — known Fabric failure mode |
| Cross-database refs (`OtherDB.dbo.Table`) | Flag as unresolved — must be resolved to Lakehouse shortcut or external table before migrating; do not silently drop |
| `WITH (MEMORY_OPTIMIZED = ON)` tables | Cannot `TRUNCATE` — use `DELETE` or `full_refresh` with drop/recreate |

## Schema Discovery

System catalog views available for metadata:

- `INFORMATION_SCHEMA.COLUMNS` — column names, types, nullability
- `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` / `KEY_COLUMN_USAGE` — primary and foreign keys
- `sys.objects` / `sys.procedures` — object metadata and creation dates
- `sys.dm_db_partition_stats` — estimated row counts (use `row_count` from `in_row_data_page_count`)

When row count estimates are available, use them to justify `incremental` over `full_refresh`.

## Load Strategy Signals (SQL Server specific)

- SP body contains `TRUNCATE TABLE` + `INSERT` → `full_refresh`
- SP body contains `INSERT` only (no delete/truncate) → `incremental` append
- SP body contains `MERGE` → prefer `full_refresh` unless target endpoint is confirmed as Warehouse
- Table has `modified_date`, `updated_at`, or `row_version` column → candidate for `incremental`
- Table has `valid_from`/`valid_to` or `is_current` flag → `snapshot`
