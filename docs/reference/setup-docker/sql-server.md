# SQL Server on Docker (macOS)

This guide sets up the local SQL Server container used by this repo.

## Container Conventions

- Container name: `aw-sql`
- Image: `mcr.microsoft.com/mssql/server:2022-latest`
- Port: `1433`
- Volume: `aw-sql-data`
- Default SA password in examples: `P@ssw0rd123`

## One-Time Setup

- Step 1: Install and start Docker Desktop.
- Step 2: Pull SQL Server image:

```bash
docker pull mcr.microsoft.com/mssql/server:2022-latest
```

- Step 3: Create container:

```bash
docker run --name aw-sql \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='P@ssw0rd123' \
  -e MSSQL_PID=Developer \
  -p 1433:1433 \
  -v aw-sql-data:/var/opt/mssql \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

- Step 4: Set restart policy:

```bash
docker update --restart unless-stopped aw-sql
```

- Step 5: Restore sample databases once:

```bash
cd /Users/hbanerjee/src/migration-utility
SA_PASSWORD='P@ssw0rd123' ./scripts/restore-dw-samples.sh
```

Databases restored by the helper script:

- `AdventureWorks2022`
- `AdventureWorksDW2022`
- `WideWorldImporters`
- `WideWorldImportersDW`

## Per Session

Start and verify:

```bash
docker start aw-sql
docker logs --tail 50 aw-sql
```

Optional connectivity check:

```bash
docker exec aw-sql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'P@ssw0rd123' -C \
  -Q "SELECT TOP 5 name FROM sys.databases ORDER BY name;"
```

Stop when done:

```bash
docker stop aw-sql
```

## Integration Test Command

```bash
cd lib && uv run pytest -m integration
```

## Troubleshooting

Container already exists:

```bash
docker start aw-sql
```

Login failure due to stale volume/password mismatch:

```bash
docker rm -f aw-sql
docker volume rm aw-sql-data
```

Then recreate container and restore samples.
