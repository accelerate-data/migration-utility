#!/usr/bin/env bash
set -euo pipefail

: "${MSSQL_HOST:=localhost}"
: "${MSSQL_PORT:=1433}"
: "${MSSQL_DB:=MigrationTest}"

echo "materialize-migration-test sql_server db=${MSSQL_DB} host=${MSSQL_HOST} port=${MSSQL_PORT}"
