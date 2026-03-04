# Fabric Warehouse Source Rules

Applies when the source system is Microsoft Fabric Warehouse (T-SQL endpoint on Fabric). These
rules supplement the shared conventions in `CLAUDE.md`.

## Fabric Warehouse Constraints

- **Warehouse vs Lakehouse endpoint**: Fabric Warehouse has a broader T-SQL surface area than the
  Lakehouse SQL Analytics Endpoint. Default migration target is the **Lakehouse endpoint**. Flag
  any object that requires a Warehouse-only feature.
- **Memory-optimized tables**: `TRUNCATE TABLE` is blocked on memory-optimized tables. Use
  `DELETE FROM` or `full_refresh` with drop/recreate semantics.
- **Cross-database references**: Fabric Warehouse federation and Lakehouse shortcuts are the two
  resolution paths. Flag every unresolved cross-database reference — do not silently drop them.
- **Row-level security / column encryption**: not portable to dbt models. Flag for manual review.

## dbt-fabric Adapter Constraints

These constraints apply to all dbt models targeting a Fabric Lakehouse endpoint via the
dbt-fabric adapter:

- **`merge` strategy silently degrades** on Lakehouse endpoints. Prefer `append` with
  `delete+insert` incremental strategy, or `full_refresh`. Only recommend `merge` when the prompt
  confirms the target is a Warehouse endpoint.
- **`datetime2` in snapshots**: causes SCD2 snapshot failures in some Fabric configurations.
  Cast to `date` or `timestamp_ntz` explicitly.
- **Composite `unique_key`**: the adapter does not support multi-column unique keys natively.
  When grain requires multiple columns, generate a surrogate key via
  `dbt_utils.generate_surrogate_key` and use that as `unique_key`.
- **Schema inference unavailable**: all output columns must be explicitly typed in generated SQL.
  Do not rely on column type inference.

## Schema Discovery

Fabric Warehouse exposes standard SQL Server system catalog views:

- `INFORMATION_SCHEMA.COLUMNS` — column names, types, nullability
- `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` / `KEY_COLUMN_USAGE` — primary and foreign keys
- `sys.objects` — object types and creation dates
- `sys.dm_db_partition_stats` — estimated row counts

## Load Strategy Signals (Fabric Warehouse specific)

- Object is a view or stored procedure wrapping a star-schema join → `full_refresh` dimension or
  fact
- Object has no `modified_date` equivalent and source volume is high → default to `full_refresh`
  with explicit note that incremental was not possible without a CDC column
- Object references memory-optimized tables → flag and use `DELETE`-based incremental or
  `full_refresh`
