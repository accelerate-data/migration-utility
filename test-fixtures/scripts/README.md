# Fixture Scripts

## csv_to_inserts.py

Extracts data from a live AdventureWorks2022 SQL Server instance and generates baseline INSERT statements for all three dialects (SQL Server, Oracle, PostgreSQL).

### Prerequisites

- SQL Server with AdventureWorks2022 accessible (e.g. via Docker: `mcr.microsoft.com/mssql/server:2022-latest`). This is the source for one-time data extraction, not the base image for the published Docker image (which pins a specific CU — see `scripts/publish-sqlserver-image.sh`).
- Python with `pyodbc` installed (available in `plugin/lib` venv)
- ODBC driver for SQL Server

### Usage

```bash
cd plugin/lib
export SA_PASSWORD=<your-sa-password>
uv run python ../../test-fixtures/scripts/csv_to_inserts.py \
    --host localhost \
    --port 1433 \
    --output-dir ../../test-fixtures/data/baseline
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--host` | `localhost` | SQL Server hostname |
| `--port` | `1433` | SQL Server port |
| `--output-dir` | (required) | Directory for output SQL files |
| `--cap` | `5000` | Max rows per table |

Requires `SA_PASSWORD` environment variable to be set.

### Output

Three files in `--output-dir`:

- `sqlserver.sql` — T-SQL with `N'string'` literals, `IDENTITY` handling
- `oracle.sql` — PL/SQL with `TO_TIMESTAMP()` literals
- `postgres.sql` — PostgreSQL with `BOOLEAN` literals

### Data sources

- **Staging tables:** Extracted directly from AdventureWorks2022 schemas (`Sales`, `Person`, `Production`, `HumanResources`)
- **stg_returns:** Synthetic — random 5% sample of order details, seeded for reproducibility
- **Dimensions:** Built from staging data with denormalized joins
- **dim_date:** Generated for the full year range of order dates
- **dim_order_status:** Static reference data (6 status codes)
