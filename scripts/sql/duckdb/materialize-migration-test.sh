#!/usr/bin/env bash
set -euo pipefail

: "${DUCKDB_PATH:=.runtime/duckdb/migrationtest.duckdb}"
mkdir -p "$(dirname "${DUCKDB_PATH}")"

echo "materialize-migration-test duckdb path=${DUCKDB_PATH}"
