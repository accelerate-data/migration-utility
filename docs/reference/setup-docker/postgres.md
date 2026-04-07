# PostgreSQL on Docker (macOS)

This guide sets up the local PostgreSQL container used by this repo.

## Container Conventions

- Container name: `pg-test`
- Image: `postgres:16`
- Port: `5432`
- Default password in examples: `postgres`

## One-Time Setup

- Step 1: Install and start Docker Desktop.

- Step 2: Pull PostgreSQL image:

```bash
docker pull postgres:16
```

- Step 3: Create container:

```bash
docker run --name pg-test \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -v pg-test-data:/var/lib/postgresql/data \
  -d postgres:16
```

- Step 4: Set restart policy:

```bash
docker update --restart unless-stopped pg-test
```

The container starts with a default `postgres` database. The Kimball fixture database (`kimball_fixture`) is loaded from the GHCR image — see [Kimball Fixture Setup](kimball-fixture.md).

## Per Session

Start and verify:

```bash
docker start pg-test
docker logs --tail 20 pg-test
```

Optional connectivity check:

```bash
docker exec pg-test psql -U postgres -c "SELECT current_database(), version();"
```

Stop when done:

```bash
docker stop pg-test
```

## Connection Details

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| User | `postgres` |
| Password | `postgres` |

## Troubleshooting

Container already exists:

```bash
docker start pg-test
```

Login failure due to stale volume:

```bash
docker rm -f pg-test
docker volume rm pg-test-data
```

Then recreate and reload the Kimball fixture.
