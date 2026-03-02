# DW Samples on Docker (macOS)

This guide sets up a local SQL Server container for development and test runs in this repo.

Databases restored by the helper script:

- `AdventureWorks2022`
- `AdventureWorksDW2022`
- `WideWorldImporters`
- `WideWorldImportersDW`

Container conventions used by this repo:

- Container name: `aw-sql`
- Exposed port: `1433`
- SQL Server image: `mcr.microsoft.com/mssql/server:2022-latest`

## One-time setup (per machine)

- Install Docker Desktop and make sure it is running.

- Pull the SQL Server image:

```bash
docker pull mcr.microsoft.com/mssql/server:2022-latest
```

- Verify the image exists locally:

```bash
docker images mcr.microsoft.com/mssql/server:2022-latest
```

- Create the SQL Server container:

```bash
docker run --name aw-sql \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='YourStrong!Passw0rd' \
  -e MSSQL_PID=Developer \
  -p 1433:1433 \
  -v aw-sql-data:/var/opt/mssql \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

- Set restart policy:

```bash
docker update --restart unless-stopped aw-sql
```

- Restore sample data once:

```bash
cd /Users/hbanerjee/src/migration-utility
SA_PASSWORD='YourStrong!Passw0rd' ./scripts/restore-dw-samples.sh
```

## New coding session (repeat each time)

- Start SQL Server:

```bash
docker start aw-sql
```

- Confirm container is healthy:

```bash
docker logs --tail 50 aw-sql
```

- Run tests for your changed module (example):

```bash
cargo test --manifest-path app/src-tauri/Cargo.toml source_sql
```

- Run real SQL Server integration tests (ignored by default), targeting `WideWorldImportersDW`:

```bash
MIGRATION_TEST_SQL_SERVER_HOST=127.0.0.1 \
MIGRATION_TEST_SQL_SERVER_PORT=1433 \
MIGRATION_TEST_SQL_SERVER_USER=sa \
MIGRATION_TEST_SQL_SERVER_PASSWORD='YourStrong!Passw0rd' \
MIGRATION_TEST_SQL_SERVER_DATABASE=WideWorldImportersDW \
cargo test --manifest-path app/src-tauri/Cargo.toml source_sql -- --ignored
```

### Target a different SQL Server instance for tests

`source_sql` live integration tests read SQL connection settings from environment variables at
runtime. Override these to run against a different host, credentials, or database:

- `MIGRATION_TEST_SQL_SERVER_HOST` (example: `10.0.0.25`)
- `MIGRATION_TEST_SQL_SERVER_PORT` (example: `1433`)
- `MIGRATION_TEST_SQL_SERVER_USER` (example: `sa`)
- `MIGRATION_TEST_SQL_SERVER_PASSWORD` (example: `MyStrongPassword`)
- `MIGRATION_TEST_SQL_SERVER_DATABASE` (example: `AdventureWorksDW2022`)

Example command:

```bash
MIGRATION_TEST_SQL_SERVER_HOST=10.0.0.25 \
MIGRATION_TEST_SQL_SERVER_PORT=1433 \
MIGRATION_TEST_SQL_SERVER_USER=sa \
MIGRATION_TEST_SQL_SERVER_PASSWORD='MyStrongPassword' \
MIGRATION_TEST_SQL_SERVER_DATABASE=AdventureWorksDW2022 \
cargo test --manifest-path app/src-tauri/Cargo.toml source_sql -- --ignored
```

If any variable is omitted, tests fall back to defaults in `app/src-tauri/src/source_sql.rs`
(`127.0.0.1:1433`, `sa`, `YourStrong!Passw0rd`, `WideWorldImportersDW`).

- Stop the container when done (optional):

```bash
docker stop aw-sql
```

## Optional local DB connection

Use these values in VS Code or another SQL client:

- Server: `localhost,1433`
- User: `sa`
- Password: your `MSSQL_SA_PASSWORD`
- Trust server certificate: enabled

## Script behavior and overrides

`scripts/restore-dw-samples.sh`:

- Downloads backups to `/tmp/sql-backups` by default.
- Restores only missing sample databases.
- Removes downloaded backups after run by default.
- Uses:
  - `SQL_SERVER_HOST` (default `localhost`)
  - `SQL_SERVER_PORT` (default `1433`)

Optional script overrides:

- `CONTAINER_NAME` (default `aw-sql`)
- `BACKUP_DIR` (default `/tmp/sql-backups`)
- `KEEP_BACKUPS=1` to keep `.bak` files

## Troubleshooting

- `Conflict. The container name "/aw-sql" is already in use`:
  - Container already exists. Use `docker start aw-sql`.
- `Login failed for user 'sa'`:
  - Usually password mismatch with persisted volume.
  - Reset container and volume:

```bash
docker rm -f aw-sql
docker volume rm aw-sql-data
```
