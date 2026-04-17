#!/usr/bin/env bash
set -euo pipefail

_require_var() {
  if [[ -z "${!1:-}" ]]; then
    echo "Required environment variable $1 is not set. Check .envrc and .env" >&2
    exit 1
  fi
}

_require_var MSSQL_HOST
_require_var MSSQL_PORT
_require_var MSSQL_DB
_require_var MSSQL_SCHEMA
_require_var MSSQL_USER
_require_var SA_PASSWORD

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

from shared.db_connect import SQL_SERVER_ODBC_DRIVER, build_sql_server_connection_string

try:
    import pyodbc
except ImportError as exc:
    raise SystemExit(
        "sqlcmd is not installed and python package 'pyodbc' is unavailable for SQL Server materialization"
    ) from exc

sql_path = Path(sys.argv[1])
database_name = sys.argv[2]

def _require_env(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit(f"Required environment variable {name} is not set. Check .envrc and .env")
    return value

conn = pyodbc.connect(
    build_sql_server_connection_string(
        host=_require_env("MSSQL_HOST"),
        port=_require_env("MSSQL_PORT"),
        database=database_name,
        user=os.environ.get("MSSQL_USER", "sa"),
        password=os.environ["SA_PASSWORD"],
        driver=SQL_SERVER_ODBC_DRIVER,
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
