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
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SQL_FILE="${REPO_ROOT}/scripts/sql/create-migration-test-db.sql"

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

escaped_schema="${MSSQL_SCHEMA//\'/\'\'}"
OBJECTS_EXIST_SQL="$(cat <<SQL
SET NOCOUNT ON;
DECLARE @schema sysname = N'${escaped_schema}';
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = @schema AND t.name = 'bronze_currency'
    ) AND EXISTS (
        SELECT 1
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = @schema AND t.name = 'silver_dimcurrency'
    ) AND EXISTS (
        SELECT 1
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = @schema AND t.name = 'silver_config'
    ) AND EXISTS (
        SELECT 1
        FROM sys.views AS v
        JOIN sys.schemas AS s ON s.schema_id = v.schema_id
        WHERE s.name = @schema AND v.name = 'silver_vw_dimpromotion'
    ) AND EXISTS (
        SELECT 1
        FROM sys.procedures AS p
        JOIN sys.schemas AS s ON s.schema_id = p.schema_id
        WHERE s.name = @schema AND p.name = 'silver_usp_load_dimcurrency'
    ) AND EXISTS (
        SELECT 1
        FROM sys.procedures AS p
        JOIN sys.schemas AS s ON s.schema_id = p.schema_id
        WHERE s.name = @schema AND p.name = 'silver_usp_unionall'
    )
THEN 1 ELSE 0 END;
SQL
)"

echo "materialize-migration-test sql_server db=${MSSQL_DB} schema=${MSSQL_SCHEMA} host=${MSSQL_HOST} port=${MSSQL_PORT}"
if [[ ${#SQLCMD[@]} -gt 0 ]]; then
  OBJECTS_EXIST="$("${SQLCMD[@]}" -d "${MSSQL_DB}" -h -1 -W -Q "${OBJECTS_EXIST_SQL}" | tr -d '\r' | tail -n 1 | xargs)"
  if [[ "${OBJECTS_EXIST}" == "1" ]]; then
    echo "MigrationTest fixture already exists in ${MSSQL_DB}.${MSSQL_SCHEMA}; leaving it in place"
    exit 0
  fi
  "${SQLCMD[@]}" -d "${MSSQL_DB}" -i "${SQL_INPUT}"
  exit 0
fi

python - <<'PY' "${SQL_INPUT}" "${MSSQL_DB}" "${MSSQL_SCHEMA}"
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
database_name = sys.argv[2]
schema_name = sys.argv[3]

conn = pyodbc.connect(
    (
        f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},{os.environ.get('MSSQL_PORT', '1433')};"
        f"DATABASE={database_name};"
        f"UID={os.environ.get('MSSQL_USER', 'sa')};PWD={os.environ['SA_PASSWORD']};"
        "TrustServerCertificate=yes;"
    ),
    autocommit=True,
)

try:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT CASE WHEN "
        "    EXISTS ("
        "        SELECT 1 "
        "        FROM sys.tables AS t "
        "        JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "        WHERE s.name = ? AND t.name = 'bronze_currency'"
        "    ) AND EXISTS ("
        "        SELECT 1 "
        "        FROM sys.tables AS t "
        "        JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "        WHERE s.name = ? AND t.name = 'silver_dimcurrency'"
        "    ) AND EXISTS ("
        "        SELECT 1 "
        "        FROM sys.tables AS t "
        "        JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "        WHERE s.name = ? AND t.name = 'silver_config'"
        "    ) AND EXISTS ("
        "        SELECT 1 "
        "        FROM sys.views AS v "
        "        JOIN sys.schemas AS s ON s.schema_id = v.schema_id "
        "        WHERE s.name = ? AND v.name = 'silver_vw_dimpromotion'"
        "    ) AND EXISTS ("
        "        SELECT 1 "
        "        FROM sys.procedures AS p "
        "        JOIN sys.schemas AS s ON s.schema_id = p.schema_id "
        "        WHERE s.name = ? AND p.name = 'silver_usp_load_dimcurrency'"
        "    ) AND EXISTS ("
        "        SELECT 1 "
        "        FROM sys.procedures AS p "
        "        JOIN sys.schemas AS s ON s.schema_id = p.schema_id "
        "        WHERE s.name = ? AND p.name = 'silver_usp_unionall'"
        "    ) "
        "THEN 1 ELSE 0 END",
        schema_name,
        schema_name,
        schema_name,
        schema_name,
        schema_name,
        schema_name,
    )
    if cursor.fetchone()[0] == 1:
        print(
            f"MigrationTest fixture already exists in {database_name}.{schema_name}; "
            "leaving it in place"
        )
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
