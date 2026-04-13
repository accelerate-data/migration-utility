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

declare -a REQUIRED_FIXTURE_OBJECTS=(
  "table:bronze_currency"
  "table:bronze_product"
  "table:silver_config"
  "table:silver_dimcurrency"
  "table:silver_dimproduct"
  "table:silver_dimpromotion"
  "table:silver_factinternetsales"
  "table:silver_dimsalesterritory"
  "view:silver_vw_dimpromotion"
  "view:silver_vdimsalesterritory"
  "procedure:silver_usp_load_dimcurrency"
  "procedure:silver_usp_load_dimproduct"
  "procedure:silver_usp_load_dimpromotion"
  "procedure:silver_usp_unionall"
)

OBJECTS_EXIST_SQL="$(python - <<'PY' "${MSSQL_SCHEMA}" "${REQUIRED_FIXTURE_OBJECTS[@]}"
import sys

schema_name = sys.argv[1].replace("'", "''")
object_specs = sys.argv[2:]
catalogs = {
    "table": ("sys.tables", "t", "name"),
    "view": ("sys.views", "v", "name"),
    "procedure": ("sys.procedures", "p", "name"),
}

clauses: list[str] = []
for spec in object_specs:
    object_kind, object_name = spec.split(":", 1)
    catalog, alias, name_column = catalogs[object_kind]
    escaped_name = object_name.replace("'", "''")
    clauses.append(
        "EXISTS ("
        " SELECT 1"
        f" FROM {catalog} AS {alias}"
        " JOIN sys.schemas AS s ON s.schema_id = "
        f"{alias}.schema_id"
        f" WHERE s.name = N'{schema_name}' AND {alias}.{name_column} = N'{escaped_name}'"
        ")"
    )

print("SET NOCOUNT ON;")
print("SELECT CASE WHEN")
for index, clause in enumerate(clauses):
    prefix = "    " if index == 0 else " AND "
    print(f"{prefix}{clause}")
print("THEN 1 ELSE 0 END;")
PY
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

python - <<'PY' "${SQL_INPUT}" "${MSSQL_DB}" "${MSSQL_SCHEMA}" "${OBJECTS_EXIST_SQL}"
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
objects_exist_sql = sys.argv[4]

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
    cursor.execute(objects_exist_sql)
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
