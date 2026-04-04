# Data Reconciliation

Row-level A-B / B-A comparison between SQL Server (source proc output) and Fabric Lakehouse (dbt model output) after parallel pipeline runs. Uses DuckDB as an in-memory comparison engine with `EXCEPT ALL` semantics.

Fixes VU-843

## Context

The migration pipeline converts SQL Server stored procedures to dbt models targeting Fabric Lakehouse. Correctness is currently validated only via dbt unit tests with synthetic fixtures. There is no post-pipeline validation that compares actual production-scale rows between the source and target after both pipelines have run.

A parallel run executes both pipelines against the same load window: the source proc writes to SQL Server, dbt writes to Fabric Lakehouse. The reconciliation step then diffs the two outputs row-by-row.

## Architecture

```text
SQL Server                              Fabric Lakehouse
(proc output)                           (dbt model output)
     |                                        |
     | pyodbc + pandas.read_sql               | DuckDB delta_scan()
     |                                        | (reads Delta/Parquet from OneLake)
     v                                        v
  ┌─────────────────────────────────────────────┐
  │              DuckDB (in-process)             │
  │                                              │
  │  src (registered DataFrame)                  │
  │       EXCEPT ALL                             │
  │  delta_scan('abfss://.../<table>')           │
  │                                              │
  │  Both directions: src-tgt, tgt-src           │
  └──────────────────┬──────────────────────────┘
                     |
                     v
              diff result (JSON)
```

Both sides feed into a single DuckDB process. SQL Server rows arrive via pyodbc as a pandas DataFrame registered in-memory (zero-copy via Arrow). Fabric Lakehouse rows are read directly from OneLake Delta tables via DuckDB's `delta` extension with `delta_scan()`.

## Why EXCEPT ALL

`EXCEPT ALL` preserves multiset semantics — if the source has two identical rows and the target has one, the diff surfaces the extra row. Plain `EXCEPT` (which SQL Server supports natively) deduplicates both sides and would miss this. DuckDB supports `EXCEPT ALL`; SQL Server does not.

The comparison is a simple set difference in both directions:

```sql
-- rows in source not in target
(SELECT * FROM src EXCEPT ALL SELECT * FROM tgt)
UNION ALL
-- rows in target not in source
(SELECT *, 'tgt_only' AS _side FROM tgt EXCEPT ALL SELECT *, 'tgt_only' FROM src)
```

No hashing, no aggregates, no join keys required. The diff returns the actual rows that differ, making debugging straightforward.

## Parallel Run Setup

A parallel run executes both the legacy pipeline and the new dbt pipeline against the same input data, then compares their outputs.

### Step 1 — Snapshot the source target schema

Before starting the parallel run, take a point-in-time snapshot of the SQL Server target tables that the source proc writes to. This establishes the baseline so the reconciliation compares only the rows produced by the current load window, not historical data that may have drifted over time.

The snapshot uses the same pattern as the existing sandbox infrastructure (`test-harness sandbox-up`):

```sql
-- Clone schema structure into a snapshot database
SELECT TOP 0 * INTO [__recon_<run_id>].dbo.<table> FROM dbo.<table>;
-- Copy current rows
INSERT INTO [__recon_<run_id>].dbo.<table> SELECT * FROM dbo.<table>;
```

This snapshot serves as the "before" state. After both pipelines run, the reconciliation diffs the incremental rows (new rows since the snapshot) on both sides.

### Step 2 — Run both pipelines

Both pipelines process the same load window concurrently:

| Pipeline | Reads from | Writes to | Engine |
|---|---|---|---|
| Source proc (ADF/Fabric pipeline) | Source tables | SQL Server target tables | SQL Server |
| dbt run | Same source data (landed in Lakehouse) | Fabric Lakehouse target tables | Spark SQL (via Fabric) |

### Step 3 — Identify incremental rows

After both pipelines complete, the incremental slice is the set of rows that appeared since the snapshot. The load window is defined by the table's watermark column (captured during the profiling stage in `catalog/tables/<table>.json` under `profile.watermark_columns`).

The reconcile command accepts a `--since` parameter (ISO timestamp) that filters both sides:

- SQL Server: `WHERE <watermark_col> >= @since`
- Lakehouse: pushed down into `delta_scan()` via DuckDB's predicate pushdown on partitioned columns, or applied as a `WHERE` filter

For the initial full load, omit `--since` to compare all rows.

### Step 4 — Diff and report

Run the reconcile command (see CLI Interface below) to pull the incremental rows from both sides and compare via `EXCEPT ALL`.

### Snapshot cleanup

After reconciliation completes, drop the snapshot database:

```sql
DROP DATABASE [__recon_<run_id>];
```

This mirrors the existing `test-harness sandbox-down` pattern.

## OneLake Connectivity

DuckDB reads Fabric Lakehouse tables directly from OneLake storage as Delta tables. No SQL Analytics Endpoint dependency.

### Path format

```text
abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table_name>
```

Use workspace and lakehouse GUIDs if names contain spaces.

### Authentication

Service principal (SPN) via DuckDB's `azure` extension:

```sql
INSTALL delta; LOAD delta;
INSTALL azure; LOAD azure;

CREATE SECRET (
    TYPE AZURE,
    PROVIDER SERVICE_PRINCIPAL,
    TENANT_ID '<tenant_id>',
    CLIENT_ID '<client_id>',
    CLIENT_SECRET '<client_secret>'
);
```

### SPN requirements

- The SPN must have at least **Viewer** role on the Fabric workspace
- The SPN must have made at least one successful call to the Fabric REST API (`GET /v1/workspaces/{id}/items`) before `delta_scan()` will work — this is a one-time bootstrap requirement documented by Microsoft

### Environment variables

| Variable | Purpose |
|---|---|
| `AZURE_TENANT_ID` | Entra ID tenant |
| `AZURE_CLIENT_ID` | SPN application (client) ID |
| `AZURE_CLIENT_SECRET` | SPN client secret |
| `ONELAKE_WORKSPACE` | Fabric workspace name or GUID |
| `ONELAKE_LAKEHOUSE` | Lakehouse name or GUID |

SQL Server connection reuses existing `MSSQL_HOST`, `MSSQL_PORT`, `SA_PASSWORD`, `MSSQL_USER` variables.

## CLI Interface

Standalone command, not embedded in `test-harness`:

```bash
uv run python -m shared.reconcile \
    --table orders \
    --since 2026-04-01T00:00:00Z \
    --output reconcile_output.json
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--table` | Yes | Table name to reconcile |
| `--since` | No | Watermark cutoff (ISO timestamp). Omit for full comparison |
| `--output` | No | Output file path (default: stdout) |
| `--exclude-columns` | No | Comma-separated columns to ignore (e.g. `_loaded_at,_etl_batch_id`) |

### Multi-table mode

```bash
uv run python -m shared.reconcile \
    --tables orders,customers,line_items \
    --since 2026-04-01T00:00:00Z
```

Tables run in parallel via a thread pool. Each table produces an independent result entry.

## Output Format

```json
{
  "reconciliation": {
    "run_id": "recon_20260404_143022",
    "timestamp": "2026-04-04T14:30:22Z",
    "since": "2026-04-01T00:00:00Z",
    "tables": [
      {
        "table": "orders",
        "status": "match",
        "src_row_count": 14523,
        "tgt_row_count": 14523,
        "src_only_count": 0,
        "tgt_only_count": 0,
        "src_only_rows": [],
        "tgt_only_rows": []
      },
      {
        "table": "line_items",
        "status": "mismatch",
        "src_row_count": 87412,
        "tgt_row_count": 87410,
        "src_only_count": 3,
        "tgt_only_count": 1,
        "src_only_rows": [
          {"order_id": 9921, "sku": "AB-100", "qty": 2, "revenue": 49.99}
        ],
        "tgt_only_rows": [
          {"order_id": 9921, "sku": "AB-100", "qty": 1, "revenue": 24.99}
        ]
      }
    ]
  }
}
```

When `src_only_count + tgt_only_count` exceeds a threshold (default 1000), the output truncates rows and reports counts only, to avoid multi-GB JSON files.

## Column Exclusion and Type Coercion

Columns that are expected to differ (ETL timestamps, batch IDs, row hashes added by one pipeline but not the other) should be excluded via `--exclude-columns`.

Type coercion between SQL Server and Lakehouse (e.g. `DATETIME2` vs `TIMESTAMP`, `DECIMAL(18,2)` vs `DOUBLE`) is handled by casting both sides to a common DuckDB type before comparison. The reconcile command reads column types from the catalog and applies a mapping:

| SQL Server type | Lakehouse (Delta) type | DuckDB common type |
|---|---|---|
| `DATETIME2` | `TIMESTAMP` | `TIMESTAMP` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | `DECIMAL(p,s)` |
| `NVARCHAR(n)` | `STRING` | `VARCHAR` |
| `BIT` | `BOOLEAN` | `BOOLEAN` |
| `INT` / `BIGINT` | `INT` / `BIGINT` | `BIGINT` |

## Dependencies

```toml
# additions to lib/pyproject.toml
dependencies = [
    "duckdb>=1.2.0",
    "deltalake>=0.24",
    "azure-identity>=1.16",
]
```

Existing dependencies (`pyodbc`, `pandas`, `pyarrow`) are already present.

## Risks and Open Questions

| Risk | Mitigation |
|---|---|
| `delta_scan()` maturity for OneLake `abfss://` paths | Validate during spike with a real Fabric Lakehouse table. Fallback: use `deltalake` Python package to read into pandas, register in DuckDB |
| SPN bootstrap requirement | Add a one-time `GET /v1/workspaces/{id}/items` call to the reconcile command's init step |
| Large tables exceed memory | Use `--since` to scope to incremental window. For full loads, batch by date range partitions |
| Type coercion mismatches cause false positives | Maintain an explicit type mapping table; allow `--exclude-columns` for known-different columns |
| DuckDB `EXCEPT ALL` performance on wide tables (50+ columns) | Acceptable for reconciliation volumes (incremental slices). Not designed for billion-row full-table scans |

## Future Extensions

These are out of scope for the current design but noted for reference:

- **Aggregate tier:** `COUNT(*)`/`SUM`/`MIN`/`MAX` pre-check before row-level diff, to fail fast on gross mismatches
- **CI integration:** run reconcile as a GitHub Actions step after parallel pipeline completion
- **Alerting:** structured output feeds into existing monitoring (Soda, GX, or custom)
- **Plugin command:** `/reconcile` skill for interactive use during migration development
