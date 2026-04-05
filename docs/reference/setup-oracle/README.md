# Oracle Setup on Docker (macOS)

This guide sets up a local Oracle Database Free (23ai) container for development and test runs.

Schemas restored by the helper script:

- `SH` — Sales History (9 tables incl. partitioned, materialized views, dimensions, ~1M rows)

Container conventions used by this repo:

- Container name: `oracle-test`
- Exposed port: `1521`
- Oracle image: `container-registry.oracle.com/database/free:latest`
- Pluggable database: `FREEPDB1`

## One-time setup (per machine)

- Install Docker Desktop and make sure it is running.

- Run the setup script:

```bash
cd /Users/hbanerjee/src/migration-utility
ORACLE_PWD='P@ssw0rd123' ./scripts/setup-oracle.sh
```

The script will:

1. Pull the Oracle Free 23ai image (~3.5 GB)
2. Create and start the `oracle-test` container
3. Wait for the database to be ready (5-10 min on first start)
4. Clone Oracle's sample schemas repo
5. Create the `sh` user in `FREEPDB1` and load the SH schema
6. Verify tables and row counts for each schema

## New coding session (repeat each time)

- Start Oracle:

```bash
docker start oracle-test
```

- Confirm container is healthy:

```bash
docker logs --tail 20 oracle-test
```

- Stop the container when done (optional):

```bash
docker stop oracle-test
```

## Connection details

| Schema | User | Password | DSN |
|---|---|---|---|
| SH | `sh` | `sh` | `localhost:1521/FREEPDB1` |
| SYS | `sys` | your `ORACLE_PWD` | `localhost:1521/FREEPDB1` |

## Python connectivity

```python
import oracledb

conn = oracledb.connect(user="sh", password="sh", dsn="localhost:1521/FREEPDB1")
```

The `oracledb` driver runs in thin mode by default — no Oracle Client install needed.

## sqlplus from container

```bash
docker exec -it oracle-test sqlplus sh/sh@FREEPDB1
```

## Environment variables

The setup script reads:

- `ORACLE_PWD` (required) — SYS/SYSTEM password
- `ORACLE_CONTAINER` (default `oracle-test`) — container name
- `ORACLE_PORT` (default `1521`) — host port

## Troubleshooting

- `name "/oracle-test" is already in use`:
  - Container already exists. Use `docker start oracle-test`.
- First start hangs beyond 10 minutes:
  - Check logs: `docker logs oracle-test`
  - On Apple Silicon, Oracle Free runs under Rosetta — ensure Docker Desktop has Rosetta enabled in Settings → General.
- `ORA-12541: TNS:no listener`:
  - Database is still initializing. Wait for `DATABASE IS READY TO USE` in logs.
- `ORA-01017: invalid username/password`:
  - SH password is `sh`. SYS password is your `ORACLE_PWD`.

## SH schema tables

| Table | Rows | Notes |
|---|---|---|
| `CHANNELS` | 5 | |
| `COUNTRIES` | 35 | |
| `CUSTOMERS` | 55,500 | |
| `PRODUCTS` | 72 | |
| `PROMOTIONS` | 503 | |
| `TIMES` | 1,826 | |
| `SUPPLEMENTARY_DEMOGRAPHICS` | 4,500 | |
| `SALES` | 918,843 | Partitioned by `time_id` |
| `COSTS` | 82,112 | Partitioned by `time_id` |

Plus: 1 view (`PROFITS`), 2 materialized views, 5 dimensions.
