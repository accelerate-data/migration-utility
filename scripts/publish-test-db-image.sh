#!/usr/bin/env bash
# Usage: ./scripts/publish-test-db-image.sh [tag]
# Runs the MigrationTest setup script (includes smoke tests), then
# commits the aw-sql Docker container and pushes to GHCR.
# Requires: SA_PASSWORD env var, docker login to ghcr.io

set -euo pipefail

TAG=${1:-latest}
IMAGE=ghcr.io/hbanerjee74/migration-test-db:${TAG}
CONTAINER=aw-sql

if [ -z "${SA_PASSWORD:-}" ]; then
    echo "ERROR: SA_PASSWORD environment variable is not set" >&2
    exit 1
fi

echo "Running MigrationTest setup script (includes smoke tests)..."
docker exec -i "${CONTAINER}" \
    /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "${SA_PASSWORD}" -No \
    < scripts/sql/create-migration-test-db.sql

echo "Smoke tests passed. Committing container as ${IMAGE}..."
docker stop "${CONTAINER}"
docker commit "${CONTAINER}" "${IMAGE}"
docker push "${IMAGE}"
docker start "${CONTAINER}"

echo "Published ${IMAGE}"
