#!/usr/bin/env bash
# Builds and publishes the SQL Server Docker image with pre-baked databases.
#
# Usage:
#   SA_PASSWORD='P@ssw0rd123' ./scripts/publish-sqlserver-image.sh [--push]
#
# The script:
# 1. Starts a temporary builder container from the pinned base image
# 2. Creates KimballFixture (schema + baseline + procedures + deltas)
# 3. Creates MigrationTest (schemas + pattern procs)
# 4. Checkpoints, shrinks logs, stops SQL Server cleanly
# 5. Extracts data files and builds the final image via Dockerfile
# 6. Optionally pushes to GHCR (with --push flag)
#
# Prerequisites:
# - Docker running
# - SA_PASSWORD environment variable set
# - For --push: docker login to ghcr.io with write:packages PAT

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Configuration ────────────────────────────────────────────────
# Pin the base image. Bump this when upgrading SQL Server CU versions,
# then re-run this script to produce data files matching the new binary.
MSSQL_TAG="2022-CU23-ubuntu-22.04"
MSSQL_BASE="mcr.microsoft.com/mssql/server:${MSSQL_TAG}"

IMAGE="ghcr.io/accelerate-data/migration-sample-sqlserver"
BUILDER_CONTAINER="sqlserver-builder-$$"
BUILD_DIR="${REPO_ROOT}/docker/sqlserver"
DATA_STAGING="${BUILD_DIR}/data"

SA_PASSWORD="${SA_PASSWORD:?SA_PASSWORD environment variable is required}"
DO_PUSH=false
if [[ "${1:-}" == "--push" ]]; then DO_PUSH=true; fi

# ── Helpers ──────────────────────────────────────────────────────
SQLCMD_BIN="/opt/mssql-tools18/bin/sqlcmd"
SQLCMD_ARGS=(-S localhost -U sa -P "$SA_PASSWORD" -C)

run_sql() {
    docker exec "$BUILDER_CONTAINER" "$SQLCMD_BIN" "${SQLCMD_ARGS[@]}" "$@"
}

run_sql_file() {
    local local_path="$1"
    local db="$2"
    local container_path="/tmp/$(basename "$local_path")"
    docker cp "$local_path" "${BUILDER_CONTAINER}:${container_path}"
    run_sql -d "$db" -i "$container_path"
}

cleanup() {
    echo "Cleaning up..."
    docker rm -f "$BUILDER_CONTAINER" 2>/dev/null || true
    rm -rf "$DATA_STAGING"
}
trap cleanup EXIT

# ── Phase 1: Start builder container ────────────────────────────
echo "Phase 1: Starting builder container from ${MSSQL_BASE}..."
docker pull "$MSSQL_BASE"
docker run --name "$BUILDER_CONTAINER" \
    -e ACCEPT_EULA=Y \
    -e MSSQL_SA_PASSWORD="$SA_PASSWORD" \
    -d "$MSSQL_BASE"

echo "Waiting for SQL Server to be ready..."
for i in $(seq 1 60); do
    if run_sql -Q "SELECT 1" &>/dev/null; then
        echo "SQL Server is ready."
        break
    fi
    if [[ $i -eq 60 ]]; then
        echo "ERROR: SQL Server did not start within 60 seconds."
        docker logs "$BUILDER_CONTAINER" 2>&1 | tail -20
        exit 1
    fi
    sleep 1
done

# ── Phase 2: Create KimballFixture ──────────────────────────────
echo "Phase 2: Creating KimballFixture database..."
run_sql -Q "CREATE DATABASE KimballFixture;"

echo "  Loading schema..."
run_sql_file "$REPO_ROOT/test-fixtures/schema/sqlserver.sql" KimballFixture

echo "  Loading baseline data (~47K rows, this may take a few minutes)..."
run_sql_file "$REPO_ROOT/test-fixtures/data/baseline/sqlserver.sql" KimballFixture

echo "  Loading stored procedures..."
run_sql_file "$REPO_ROOT/test-fixtures/procedures/sqlserver.sql" KimballFixture

echo "  Applying delta scenarios..."
for delta_dir in "$REPO_ROOT"/test-fixtures/data/delta/*/; do
    delta_name="$(basename "$delta_dir")"
    if [[ -f "$delta_dir/sqlserver.sql" ]]; then
        echo "    $delta_name"
        run_sql_file "$delta_dir/sqlserver.sql" KimballFixture
    fi
done

# ── Phase 3: Create MigrationTest ───────────────────────────────
echo "Phase 3: Creating MigrationTest database..."
run_sql -Q "CREATE DATABASE MigrationTest;"
run_sql -d MigrationTest -Q "
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='gold')   EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='staging') EXEC('CREATE SCHEMA staging');
"

# Pattern procs (from the baked-in /tmp/ files in the old image, or from repo)
if [[ -f "$REPO_ROOT/test-fixtures/procedures/pattern_procs.sql" ]]; then
    echo "  Loading pattern procs from repo..."
    run_sql_file "$REPO_ROOT/test-fixtures/procedures/pattern_procs.sql" MigrationTest
else
    echo "  No pattern_procs.sql found in test-fixtures/procedures/, skipping."
fi

# ── Phase 4: Verify ─────────────────────────────────────────────
echo "Phase 4: Verifying databases..."
run_sql -Q "
SET NOCOUNT ON;
SELECT name FROM sys.databases WHERE name IN ('KimballFixture', 'MigrationTest') ORDER BY name;
"

run_sql -d KimballFixture -Q "
SET NOCOUNT ON;
SELECT 'tables=' + CAST(COUNT(*) AS VARCHAR) FROM sys.tables;
SELECT 'procs='  + CAST(COUNT(*) AS VARCHAR) FROM sys.procedures;
SELECT 'views='  + CAST(COUNT(*) AS VARCHAR) FROM sys.views;
SELECT 'rows='   + CAST(CAST(SUM(p.rows) AS BIGINT) AS VARCHAR)
  FROM sys.tables t
  JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1);
"

# ── Phase 5: Checkpoint, shrink, shutdown ────────────────────────
echo "Phase 5: Checkpointing and shrinking log files..."
run_sql -Q "
USE KimballFixture;  CHECKPOINT; DBCC SHRINKFILE(KimballFixture_log, 1);
USE MigrationTest;   CHECKPOINT; DBCC SHRINKFILE(MigrationTest_log, 1);
"

echo "Stopping SQL Server cleanly..."
docker stop "$BUILDER_CONTAINER"

# ── Phase 6: Extract data files ─────────────────────────────────
echo "Phase 6: Extracting data files..."
rm -rf "$DATA_STAGING"
mkdir -p "$DATA_STAGING"
docker cp "${BUILDER_CONTAINER}:/var/opt/mssql/data/." "$DATA_STAGING/"

# Remove tempdb files — SQL Server recreates these on startup
rm -f "$DATA_STAGING"/tempdb*.mdf "$DATA_STAGING"/tempdb*.ndf "$DATA_STAGING"/templog.ldf

echo "Data files:"
ls -lh "$DATA_STAGING/"

# ── Phase 7: Build image ────────────────────────────────────────
DATE_TAG="$(date +%Y%m%d)"
echo "Phase 7: Building image..."
docker build \
    --build-arg "MSSQL_TAG=${MSSQL_TAG}" \
    -t "${IMAGE}:latest" \
    -t "${IMAGE}:${DATE_TAG}" \
    "$BUILD_DIR"

echo "Built: ${IMAGE}:latest and ${IMAGE}:${DATE_TAG}"

# ── Phase 8: Push (optional) ────────────────────────────────────
if [[ "$DO_PUSH" == "true" ]]; then
    echo "Phase 8: Pushing to GHCR..."
    docker push "${IMAGE}:latest"
    docker push "${IMAGE}:${DATE_TAG}"
    echo "Published ${IMAGE}:latest and ${IMAGE}:${DATE_TAG}"
else
    echo "Skipping push (run with --push to publish to GHCR)."
    echo "Local image: ${IMAGE}:latest"
fi

# Cleanup handled by trap (removes builder container and data staging dir)
echo "Done."
