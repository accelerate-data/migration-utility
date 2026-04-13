#!/usr/bin/env bash
set -euo pipefail

: "${MSSQL_HOST:=localhost}"
: "${MSSQL_PORT:=1433}"
: "${MSSQL_DB:=MigrationTest}"
: "${MSSQL_USER:=sa}"

if [[ -z "${SA_PASSWORD:-}" ]]; then
  echo "SA_PASSWORD must be set" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SQL_FILE="${REPO_ROOT}/scripts/sql/create-migration-test-db.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "SQL fixture source not found: ${SQL_FILE}" >&2
  exit 1
fi

if command -v sqlcmd >/dev/null 2>&1; then
  SQLCMD=(sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "${MSSQL_USER}" -P "${SA_PASSWORD}" -C)
elif [[ -x /opt/mssql-tools18/bin/sqlcmd ]]; then
  SQLCMD=(/opt/mssql-tools18/bin/sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "${MSSQL_USER}" -P "${SA_PASSWORD}" -C)
else
  echo "sqlcmd is required to materialize MigrationTest for SQL Server" >&2
  exit 1
fi

if [[ "${MSSQL_DB}" != "MigrationTest" ]]; then
  tmp_sql="$(mktemp)"
  trap 'rm -f "${tmp_sql}"' EXIT
  sed "s/\\<MigrationTest\\>/${MSSQL_DB}/g" "${SQL_FILE}" > "${tmp_sql}"
  SQL_INPUT="${tmp_sql}"
else
  SQL_INPUT="${SQL_FILE}"
fi

echo "materialize-migration-test sql_server db=${MSSQL_DB} host=${MSSQL_HOST} port=${MSSQL_PORT}"
"${SQLCMD[@]}" -d master -i "${SQL_INPUT}"
