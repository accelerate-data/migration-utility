#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:=localhost}"
: "${ORACLE_PORT:=1521}"
: "${ORACLE_SERVICE:=FREEPDB1}"
: "${ORACLE_USER:=sys}"
: "${ORACLE_SCHEMA:=SH}"

if [[ -z "${ORACLE_PWD:-}" ]]; then
  echo "ORACLE_PWD must be set" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
FIXTURE_SQL="${REPO_ROOT}/scripts/sql/oracle/migration_test.sql"

if [[ ! -f "${FIXTURE_SQL}" ]]; then
  echo "Oracle fixture SQL file not found: ${FIXTURE_SQL}" >&2
  exit 1
fi

CONNECT_STRING="${ORACLE_USER}/${ORACLE_PWD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}"
TMP_SQL="$(mktemp)"
trap 'rm -f "${TMP_SQL}"' EXIT
sed "s/__SCHEMA__/${ORACLE_SCHEMA}/g" "${FIXTURE_SQL}" > "${TMP_SQL}"

echo "materialize-migration-test oracle service=${ORACLE_SERVICE} host=${ORACLE_HOST} port=${ORACLE_PORT} schema=${ORACLE_SCHEMA}"
if command -v sqlplus >/dev/null 2>&1; then
  if [[ "${ORACLE_USER,,}" == "sys" ]]; then
    sqlplus -S /nolog <<SQL
CONNECT ${CONNECT_STRING} AS SYSDBA
@${TMP_SQL}
EXIT
SQL
  else
    sqlplus -S "${CONNECT_STRING}" @"${TMP_SQL}"
  fi
  exit 0
fi

python - <<'PY' "${TMP_SQL}"
import os
import re
import sys
from pathlib import Path

try:
    import oracledb
except ImportError as exc:
    raise SystemExit(
        "sqlplus is not installed and python package 'oracledb' is unavailable for Oracle materialization"
    ) from exc

sql_path = Path(sys.argv[1])
mode = (
    oracledb.AUTH_MODE_SYSDBA
    if os.environ.get("ORACLE_USER", "sys").lower() == "sys"
    else oracledb.AUTH_MODE_DEFAULT
)
conn = oracledb.connect(
    user=os.environ.get("ORACLE_USER", "sys"),
    password=os.environ["ORACLE_PWD"],
    dsn=f"{os.environ.get('ORACLE_HOST', 'localhost')}:{os.environ.get('ORACLE_PORT', '1521')}/{os.environ.get('ORACLE_SERVICE', 'FREEPDB1')}",
    mode=mode,
)

try:
    cursor = conn.cursor()
    raw = sql_path.read_text(encoding="utf-8")
    raw = "\n".join(
        line for line in raw.splitlines()
        if not line.lstrip().upper().startswith("WHENEVER ")
    )
    chunks = re.split(r"(?m)^[ \t]*/[ \t]*$", raw)
    for chunk in chunks:
        statement = chunk.strip()
        if statement:
            cursor.execute(statement)
    conn.commit()
finally:
    conn.close()
PY
