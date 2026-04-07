# Docker Setup

Pre-built Docker images with the full Kimball DW fixture baked in: schema, 20 stored procedures, ~47K baseline rows, and all 5 delta scenarios. Pull, run, and the database is immediately ready — no manual SQL loading required.

## Container Conventions

| Container | Image | Port | Purpose |
|---|---|---|---|
| `sql-test` | `ghcr.io/accelerate-data/migration-sample-sqlserver:latest` | `1433` | SQL Server — KimballFixture |
| `oracle-test` | `ghcr.io/accelerate-data/migration-sample-oracle:latest` | `1521` | Oracle 23ai — KimballFixture |
| `pg-test` | `ghcr.io/accelerate-data/migration-sample-postgres:latest` | `5432` | PostgreSQL — KimballFixture |

## One-Time Setup (per machine)

Log in to GHCR (requires a GitHub PAT with `read:packages`):

```bash
echo YOUR_GITHUB_PAT | docker login ghcr.io -u YOUR_GITHUB_USER --password-stdin
```

Pull all three images:

```bash
docker pull ghcr.io/accelerate-data/migration-sample-sqlserver:latest
docker pull ghcr.io/accelerate-data/migration-sample-oracle:latest
docker pull ghcr.io/accelerate-data/migration-sample-postgres:latest
```

Start containers:

```bash
# SQL Server
docker run --name sql-test \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='P@ssw0rd123' \
  -p 1433:1433 \
  -d ghcr.io/accelerate-data/migration-sample-sqlserver:latest

# Oracle
docker run --name oracle-test \
  -e ORACLE_PWD='P@ssw0rd123' \
  -p 1521:1521 \
  -d ghcr.io/accelerate-data/migration-sample-oracle:latest

# PostgreSQL
docker run --name pg-test \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d ghcr.io/accelerate-data/migration-sample-postgres:latest
```

Set restart policy:

```bash
docker update --restart unless-stopped sql-test oracle-test pg-test
```

## Quick Start (per session)

```bash
docker start sql-test oracle-test pg-test
```

Wait ~30 seconds for Oracle to finish startup:

```bash
docker logs oracle-test 2>&1 | tail -5
# Ready when you see: DATABASE IS READY TO USE
```

Stop when done:

```bash
docker stop sql-test oracle-test pg-test
```

## Connection Details

### SQL Server

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `1433` |
| Database | `KimballFixture` |
| User | `sa` |
| Password | `P@ssw0rd123` |

```bash
docker exec sql-test /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'P@ssw0rd123' -C \
  -d KimballFixture \
  -Q "SELECT COUNT(*) FROM dim.dim_customer;"
```

### Oracle

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `1521` |
| Service | `FREEPDB1` |
| Schema | `kimball` |
| User | `kimball` |
| Password | `kimball` |

```bash
docker exec oracle-test bash -c \
  "echo 'SELECT COUNT(*) FROM dim_customer;' | sqlplus -S kimball/kimball@FREEPDB1"
```

### PostgreSQL

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `kimball_fixture` |
| User | `postgres` |
| Password | `postgres` |

```bash
docker exec pg-test psql -U postgres -d kimball_fixture \
  -c "SELECT COUNT(*) FROM dim.dim_customer;"
```

## Applying a Delta Scenario Manually

Delta SQL files are in `test-fixtures/data/delta/NN-<name>/`. Example for delta 01:

```bash
# SQL Server
docker cp test-fixtures/data/delta/01-new-customer-product/sqlserver.sql sql-test:/tmp/delta01.sql
docker exec sql-test /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'P@ssw0rd123' -C -d KimballFixture -i /tmp/delta01.sql

# Oracle
docker cp test-fixtures/data/delta/01-new-customer-product/oracle.sql oracle-test:/tmp/delta01.sql
docker exec oracle-test bash -c "sqlplus -S kimball/kimball@FREEPDB1 @/tmp/delta01.sql"

# PostgreSQL
docker cp test-fixtures/data/delta/01-new-customer-product/postgres.sql pg-test:/tmp/delta01.sql
docker exec pg-test psql -U postgres -d kimball_fixture -f /tmp/delta01.sql
```

After applying, run `usp_exec_orchestrator_full_load` to process the staged changes.

## Running Parity Validation

After all three containers are running:

```bash
uv run test-fixtures/parity/validate.py
```

See [`test-fixtures/parity/README.md`](../../../test-fixtures/parity/README.md) for details.

## `.env` Variables for MCP Servers

Add to `.env` if running MCP server connections against the Kimball fixture:

```bash
# SQL Server (KimballFixture)
MSSQL_HOST=127.0.0.1
MSSQL_PORT=1433
MSSQL_DB=KimballFixture

# Oracle
ORACLE_USER=kimball
ORACLE_PWD=kimball
ORACLE_DSN=localhost:1521/FREEPDB1

# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DB=kimball_fixture
PG_USER=postgres
PG_PASSWORD=postgres
```
