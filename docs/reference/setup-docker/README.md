# Docker Setup

All Docker containers used by this repo. Start here — each section links to dialect-specific setup.

## Container Conventions

| Container | Image | Port | Purpose |
|---|---|---|---|
| `sql-test` | `mcr.microsoft.com/mssql/server:2022-latest` | `1433` | SQL Server — AdventureWorks + KimballFixture |
| `oracle-test` | `container-registry.oracle.com/database/free:latest` | `1521` | Oracle 23ai — SH schema + KimballFixture |
| `pg-test` | `postgres:16` | `5432` | PostgreSQL — KimballFixture |

## Quick Start (per session)

```bash
docker start sql-test oracle-test pg-test
```

Stop when done:

```bash
docker stop sql-test oracle-test pg-test
```

## Setup Guides

| Guide | Contents |
|---|---|
| [SQL Server](sql-server.md) | One-time setup of `sql-test`; AdventureWorks + WideWorldImporters restore |
| [Oracle](../../reference/setup-oracle/README.md) | One-time setup of `oracle-test`; SH schema load; SQLcl MCP prereqs |
| [PostgreSQL](postgres.md) | One-time setup of `pg-test` |
| [Kimball Fixture (GHCR)](kimball-fixture.md) | Pull Kimball fixture images from GHCR; Kimball-specific connection strings |
| [Test Database Image](../test-db-image/README.md) | Building and publishing the `migration-test-db` GHCR image used by CI |
