# Fabric Lakehouse Source Rules

Applies when the source system is Microsoft Fabric Lakehouse (Delta tables on OneLake). These
rules supplement the shared conventions in `CLAUDE.md`.

## What Is in Scope

Only **Delta table metadata and schema** from the Lakehouse are in scope. Spark notebooks,
Python transformation code, and Dataflow Gen2 pipelines that populate those tables are **out of
scope** — do not attempt to migrate notebook logic.

## Schema Discovery

Fabric Lakehouse Delta tables expose schema via:

- Lakehouse SQL Analytics Endpoint (`INFORMATION_SCHEMA.COLUMNS`) — column names and types
- Delta table transaction log (`_delta_log/`) — partition columns, schema evolution history
- Fabric REST API (`/tables/{tableName}/schema`) — when direct SQL access is unavailable

Column types from the SQL endpoint use SQL Server type aliases. Map to dbt-fabric target types
explicitly — do not rely on inference.

## Type Mapping (Spark → dbt-fabric SQL)

| Spark / Delta type | dbt-fabric SQL type |
|---|---|
| `LongType` / `bigint` | `bigint` |
| `IntegerType` / `int` | `int` |
| `StringType` / `string` | `varchar(max)` |
| `TimestampType` | `datetime2` (or `timestamp_ntz` where supported) |
| `DateType` | `date` |
| `DoubleType` / `float` | `float` |
| `DecimalType(p,s)` | `decimal(p,s)` |
| `BooleanType` / `boolean` | `bit` |

## Partitioning and Load Strategy

- **Partitioned Delta tables** with a date-based partition column are strong candidates for
  `incremental` — use the partition column as `incremental_column`.
- **Non-partitioned tables** with a `modified_date` or `updated_at` column → `incremental`.
- **Non-partitioned tables without a CDC column** → `full_refresh`.
- **SCD2-style tables** with `valid_from`/`valid_to` or `is_current` → `snapshot`.

## Load Strategy Signals (Fabric Lakehouse specific)

- Delta table has partition columns → prefer `incremental`; use partition column as
  `incremental_column` if it is date-typed
- Delta table schema shows `_change_data_feed` properties → CDC available; use `incremental`
- Table name or column pattern suggests history (`History`, `SCD`, `valid_from`) → `snapshot`
- Table has no natural ordering or CDC column, and is small → `full_refresh`

## dbt-fabric Adapter Constraints

Same constraints as Fabric Warehouse source — see `source-fabric-warehouse.md` for the dbt-fabric
adapter rules that apply to the Lakehouse target endpoint.
