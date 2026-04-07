# Test Database Image

The agent integration tests run against a pre-built Docker image that contains SQL Server with
two databases already loaded:

- `AdventureWorks2022` — source data (OLTP)
- `MigrationTest` — bronze/silver schemas with scenario procedures for all agent test suites

Image: `ghcr.io/hbanerjee74/migration-test-db:latest`

The image is built locally and published to GHCR manually. CI pulls and starts it — CI does not
rebuild it.

---

## When to publish a new version

Publish when any of the following change:

- `scripts/sql/create-migration-test-db.sql` — schema, procedures, or data
- A new test scenario is added to `scripts/sql/test-fixtures/`
- The `AdventureWorks2022` source data changes on the local container

---

## Prerequisites (one-time per machine)

- Docker Desktop running with the `sql-test` container started (see
  [Docker Setup](../setup-docker/README.md)).
- `AdventureWorks2022` restored in that container (required by the setup script).
- A GitHub PAT with `write:packages` scope. Create one at
  `GitHub → Settings → Developer settings → Personal access tokens`.

---

## Steps to publish

### 1. Verify the container is in the correct state

Run the setup script to rebuild `MigrationTest` from scratch and confirm all smoke tests pass:

```bash
SA_PASSWORD='P@ssw0rd123' docker exec -i sql-test \
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -No \
  < scripts/sql/create-migration-test-db.sql
```

All lines should end with `PASS`. The final line should be `=== Smoke tests passed ===`.

### 2. Log in to GHCR

```bash
echo YOUR_GITHUB_PAT | docker login ghcr.io -u hbanerjee74 --password-stdin
```

### 3. Commit the container as the new image

```bash
docker stop sql-test
docker commit sql-test ghcr.io/hbanerjee74/migration-test-db:latest
docker start sql-test
```

This snapshots the full container state — both `AdventureWorks2022` and `MigrationTest` are
baked into the image. No restore or setup script is needed when the image is started.

### 4. Push to GHCR

```bash
docker push ghcr.io/hbanerjee74/migration-test-db:latest
```

The first push transfers the full image (~4–5 GB). Subsequent pushes only transfer changed layers.

### 5. Tag a versioned release (optional but recommended when scenarios change)

```bash
docker tag ghcr.io/hbanerjee74/migration-test-db:latest \
           ghcr.io/hbanerjee74/migration-test-db:$(date +%Y%m%d)
docker push ghcr.io/hbanerjee74/migration-test-db:$(date +%Y%m%d)
```

Update the image tag reference in test configuration when pinning CI to a specific version.

---

## Using the image in tests

Pull and start:

```bash
docker pull ghcr.io/hbanerjee74/migration-test-db:latest

docker run --name test-db \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='P@ssw0rd123' \
  -p 1433:1433 \
  -d ghcr.io/hbanerjee74/migration-test-db:latest
```

Both databases are immediately available. No restore step needed.

---

## Scenario coverage in MigrationTest

| Silver table | Scoping scenario | Expected status |
|---|---|---|
| `silver.DimProduct` | Direct MERGE writer | `resolved` |
| `silver.DimCustomer` | Two writers (Full + Delta) | `ambiguous_multi_writer` |
| `silver.FactInternetSales` | Orchestrator calls staging proc | `resolved` (call graph) |
| `silver.DimGeography` | No loader proc | `no_writer_found` |
| `silver.DimCurrency` | All writes via `sp_executesql` | `partial` |
| `silver.DimEmployee` | Callee references `[OtherDB]` | `error` (cross-db) |
| `silver.DimPromotion` | Writes through updateable view | `resolved` (writer-through-view) |
| `silver.DimSalesTerritory` | Indexed view as target | `resolved` (MV-as-target) |

Test input fixtures for each scenario: `scripts/sql/test-fixtures/`.
