#!/usr/bin/env bash
set -euo pipefail

: "${MSSQL_HOST:=localhost}"
: "${MSSQL_PORT:=1433}"
: "${MSSQL_DB:=AdventureWorks2022}"
: "${MSSQL_SCHEMA:=MigrationTest}"
: "${MSSQL_USER:=sa}"

if [[ -z "${SA_PASSWORD:-}" ]]; then
  echo "SA_PASSWORD must be set" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
SQL_FILE="${REPO_ROOT}/tests/integration/sql_server/fixtures/create-migration-test.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "SQL fixture source not found: ${SQL_FILE}" >&2
  exit 1
fi

tmp_sql="$(mktemp)"
trap 'rm -f "${tmp_sql}"' EXIT
python - <<'PY' "${SQL_FILE}" "${tmp_sql}" "${MSSQL_DB}" "${MSSQL_SCHEMA}"
from pathlib import Path
import sys

source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
database_name = sys.argv[3]
schema_name = sys.argv[4]

sql_text = source_path.read_text(encoding="utf-8")
sql_text = sql_text.replace("__MSSQL_DB__", database_name)
sql_text = sql_text.replace("__MSSQL_SCHEMA__", schema_name)
target_path.write_text(sql_text, encoding="utf-8")
PY
SQL_INPUT="${tmp_sql}"

if command -v sqlcmd >/dev/null 2>&1; then
  SQLCMD=(sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "${MSSQL_USER}" -P "${SA_PASSWORD}" -C)
elif [[ -x /opt/mssql-tools18/bin/sqlcmd ]]; then
  SQLCMD=(/opt/mssql-tools18/bin/sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "${MSSQL_USER}" -P "${SA_PASSWORD}" -C)
else
  SQLCMD=()
fi

echo "materialize-migration-test sql_server db=${MSSQL_DB} schema=${MSSQL_SCHEMA} host=${MSSQL_HOST} port=${MSSQL_PORT}"
if [[ ${#SQLCMD[@]} -gt 0 ]]; then
  "${SQLCMD[@]}" -d "${MSSQL_DB}" -i "${SQL_INPUT}"
  exit 0
fi

python - <<'PY' "${SQL_INPUT}" "${MSSQL_DB}" "${REPO_ROOT}"
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(sys.argv[3]) / "lib"))

from shared.db_connect import build_sql_server_connection_string

try:
    import pyodbc
except ImportError as exc:
    raise SystemExit(
        "sqlcmd is not installed and python package 'pyodbc' is unavailable for SQL Server materialization"
    ) from exc

sql_path = Path(sys.argv[1])
database_name = sys.argv[2]

conn = pyodbc.connect(
    build_sql_server_connection_string(
        host=os.environ.get("MSSQL_HOST", "localhost"),
        port=os.environ.get("MSSQL_PORT", "1433"),
        database=database_name,
        user=os.environ.get("MSSQL_USER", "sa"),
        password=os.environ["SA_PASSWORD"],
        driver=os.environ.get("MSSQL_DRIVER", "FreeTDS"),
    ),
    autocommit=True,
)

try:
    cursor = conn.cursor()
    sql_text = sql_path.read_text(encoding="utf-8")
    batches = re.split(r"(?im)^[ \t]*GO[ \t]*$", sql_text)
    for batch in batches:
        statement = batch.strip()
        if statement:
            cursor.execute(statement)
finally:
    conn.close()
PY
