# Parity Validation

One-time cross-dialect parity check for the Kimball fixture. Validates that all three dialect environments produce identical output after the full load pipeline, establishing a reliable baseline before parallel-run simulation.

## Prerequisites

All three containers must be running with the Kimball fixture loaded. See [Docker Setup](../../docs/reference/setup-docker/README.md) for pull and run instructions.

```bash
docker start sql-test
docker start oracle-test
docker start pg-test
```

`uv` must be installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`).

## Running Validation

From the repo root:

```bash
uv run test-fixtures/parity/validate.py
```

The script:

1. Connects to all three containers (SQL Server, Oracle, PostgreSQL)
2. Runs `usp_exec_orchestrator_full_load` on each dialect (baseline)
3. Compares all 20 output tables — SQL Server is the reference
4. Applies each of 5 delta scenarios, re-runs the orchestrator, and compares again

Total: 6 rounds × 20 tables = 120 table comparisons.

### Validate a single table

```bash
uv run test-fixtures/parity/validate.py --table dim.dim_customer
```

## Output

Each round prints a line per table:

```text
  PASS  dim.dim_customer  ref=5000
  FAIL  gold.rpt_channel_pivot  ref=11 cmp=0 diff_ref=11 diff_cmp=0
    SQL Server-only rows (first 5):
      {"channel": "Online", ...}
```

Exit code `0` = all rounds pass. Exit code `1` = any mismatch or connection failure.

## Normalization Rules

Before comparison, all rows are normalized:

| Rule | Detail |
|---|---|
| Column names | Lowercased (Oracle returns uppercase metadata) |
| Dates | ISO 8601 date string (`YYYY-MM-DD`) |
| Decimals | Rounded to 2 decimal places |
| NULLs | Replaced with `"__NULL__"` sentinel |
| Booleans | Coerced to `0`/`1` (normalizes PG `BOOLEAN` vs SQL Server `BIT`) |
| Row order | Sorted by JSON-serialized row content (order-independent) |

## Known Edge Cases

- **`rpt_channel_pivot`** requires `fct_sales_by_channel` to be populated — the orchestrator loads facts in the correct order, so this resolves automatically.
- **`fct_sales`** has two writers (`usp_load_fct_sales_daily` + `usp_load_fct_sales_historical`). The orchestrator calls daily first, then historical (TRUNCATE+INSERT), so the final state is the historical full rebuild.
- **`rpt_product_margin` and `rpt_date_sales_rollup`** are both written by `usp_load_gold_agg_batch` — both are compared individually.

## Artifacts

- [`coverage-matrix.md`](coverage-matrix.md) — procedure → pattern category mapping
- [`validate.py`](validate.py) — the validation script (PEP 723 inline deps)

## Connection Details

| Dialect | Container | Host | Database | Credentials |
|---|---|---|---|---|
| SQL Server | `sql-test` | `localhost:1433` | `KimballFixture` | `sa` / `P@ssw0rd123` |
| Oracle | `oracle-test` | `localhost:1521/FREEPDB1` | `kimball` schema | `kimball` / `kimball` |
| PostgreSQL | `pg-test` | `localhost:5432` | `kimball_fixture` | `postgres` / `postgres` |
