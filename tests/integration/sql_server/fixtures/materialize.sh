#!/usr/bin/env bash
set -euo pipefail

_require_var() {
  if [[ -z "${!1:-}" ]]; then
    echo "Required environment variable $1 is not set. Check .envrc and .env" >&2
    exit 1
  fi
}

_require_var SOURCE_MSSQL_HOST
_require_var SOURCE_MSSQL_PORT
_require_var SOURCE_MSSQL_DB
_require_var SOURCE_MSSQL_SCHEMA
_require_var SANDBOX_MSSQL_USER
_require_var SANDBOX_MSSQL_PASSWORD

: "${SOURCE_MSSQL_USER:=}"
: "${SOURCE_MSSQL_PASSWORD:=}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
SQL_FILE="${REPO_ROOT}/tests/integration/sql_server/fixtures/create-migration-test.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "SQL fixture source not found: ${SQL_FILE}" >&2
  exit 1
fi

tmp_sql="$(mktemp)"
trap 'rm -f "${tmp_sql}"' EXIT
python - <<'PY' "${SQL_FILE}" "${tmp_sql}" "${SOURCE_MSSQL_DB}" "${SOURCE_MSSQL_SCHEMA}" "${SOURCE_MSSQL_USER}" "${SOURCE_MSSQL_PASSWORD}"
from pathlib import Path
import sys

source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
database_name = sys.argv[3]
schema_name = sys.argv[4]
source_user = sys.argv[5]
source_password = sys.argv[6]


def quote_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def quote_identifier(value: str) -> str:
    return value.replace("]", "]]")


sql_text = source_path.read_text(encoding="utf-8")
sql_text = sql_text.replace("__SOURCE_MSSQL_DB__", database_name)
sql_text = sql_text.replace("__SOURCE_MSSQL_SCHEMA__", schema_name)
sql_text = sql_text.replace("__SOURCE_MSSQL_USER__", quote_identifier(source_user))
sql_text = sql_text.replace("__SOURCE_MSSQL_USER_LITERAL__", quote_sql_literal(source_user))
sql_text = sql_text.replace("__SOURCE_MSSQL_PASSWORD_LITERAL__", quote_sql_literal(source_password))
target_path.write_text(sql_text, encoding="utf-8")
PY
SQL_INPUT="${tmp_sql}"

if command -v sqlcmd >/dev/null 2>&1; then
  SQLCMD=(sqlcmd -S "${SOURCE_MSSQL_HOST},${SOURCE_MSSQL_PORT}" -U "${SANDBOX_MSSQL_USER}" -P "${SANDBOX_MSSQL_PASSWORD}" -C)
elif [[ -x /opt/mssql-tools18/bin/sqlcmd ]]; then
  SQLCMD=(/opt/mssql-tools18/bin/sqlcmd -S "${SOURCE_MSSQL_HOST},${SOURCE_MSSQL_PORT}" -U "${SANDBOX_MSSQL_USER}" -P "${SANDBOX_MSSQL_PASSWORD}" -C)
else
  SQLCMD=()
fi

echo "materialize-migration-test sql_server db=${SOURCE_MSSQL_DB} schema=${SOURCE_MSSQL_SCHEMA} host=${SOURCE_MSSQL_HOST} port=${SOURCE_MSSQL_PORT}"
if [[ ${#SQLCMD[@]} -gt 0 ]]; then
  "${SQLCMD[@]}" -d "${SOURCE_MSSQL_DB}" -i "${SQL_INPUT}"
  exit 0
fi

python - <<'PY' "${SQL_INPUT}" "${SOURCE_MSSQL_DB}" "${REPO_ROOT}"
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
        host=_require_env("SOURCE_MSSQL_HOST"),
        port=_require_env("SOURCE_MSSQL_PORT"),
        database=database_name,
        user=_require_env("SANDBOX_MSSQL_USER"),
        password=_require_env("SANDBOX_MSSQL_PASSWORD"),
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
