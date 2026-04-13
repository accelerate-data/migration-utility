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
  SQLCMD=()
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
if [[ ${#SQLCMD[@]} -gt 0 ]]; then
  DB_EXISTS="$("${SQLCMD[@]}" -d master -h -1 -W -Q "SET NOCOUNT ON; SELECT CASE WHEN DB_ID(N'${MSSQL_DB}') IS NULL THEN 0 ELSE 1 END;" | tr -d '\r' | tail -n 1 | xargs)"
  if [[ "${DB_EXISTS}" == "1" ]]; then
    echo "MigrationTest database ${MSSQL_DB} already exists; leaving it in place"
    exit 0
  fi
  "${SQLCMD[@]}" -d master -i "${SQL_INPUT}"
  exit 0
fi

python - <<'PY' "${SQL_INPUT}"
import os
import re
import sys
from pathlib import Path

try:
    import pyodbc
except ImportError as exc:
    raise SystemExit(
        "sqlcmd is not installed and python package 'pyodbc' is unavailable for SQL Server materialization"
    ) from exc

sql_path = Path(sys.argv[1])

conn = pyodbc.connect(
    (
        f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},{os.environ.get('MSSQL_PORT', '1433')};"
        "DATABASE=master;"
        f"UID={os.environ.get('MSSQL_USER', 'sa')};PWD={os.environ['SA_PASSWORD']};"
        "TrustServerCertificate=yes;"
    ),
    autocommit=True,
)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT CASE WHEN DB_ID(?) IS NULL THEN 0 ELSE 1 END", os.environ.get("MSSQL_DB", "MigrationTest"))
    if cursor.fetchone()[0] == 1:
        print(f"MigrationTest database {os.environ.get('MSSQL_DB', 'MigrationTest')} already exists; leaving it in place")
        raise SystemExit(0)

    sql_text = sql_path.read_text(encoding="utf-8")
    batches = re.split(r"(?im)^[ \t]*GO[ \t]*$", sql_text)
    for batch in batches:
        statement = batch.strip()
        if statement:
            cursor.execute(statement)
finally:
    conn.close()
PY
