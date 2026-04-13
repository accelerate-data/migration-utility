#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:=localhost}"
: "${ORACLE_PORT:=1521}"
: "${ORACLE_SERVICE:=FREEPDB1}"
: "${ORACLE_USER:=sys}"
: "${ORACLE_SCHEMA:=MIGRATIONTEST}"
: "${ORACLE_SCHEMA_PASSWORD:=${ORACLE_SCHEMA,,}}"

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
ORACLE_CLI="${SQLCL_BIN:-}"
if [[ -z "${ORACLE_CLI}" ]] && command -v sql >/dev/null 2>&1; then
  ORACLE_CLI="$(command -v sql)"
fi
if [[ -z "${ORACLE_CLI}" ]] && command -v sqlplus >/dev/null 2>&1; then
  ORACLE_CLI="$(command -v sqlplus)"
fi

if [[ -n "${ORACLE_CLI}" ]]; then
  CONNECT_DIRECTIVE="CONNECT ${ORACLE_USER}/\"${ORACLE_PWD}\"@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}"
  if [[ "${ORACLE_USER,,}" == "sys" ]]; then
    CONNECT_DIRECTIVE="${CONNECT_DIRECTIVE} AS SYSDBA"
  fi
  "${ORACLE_CLI}" -S /nolog <<SQL
${CONNECT_DIRECTIVE}
DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM ALL_USERS WHERE USERNAME = UPPER('${ORACLE_SCHEMA}');
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER "${ORACLE_SCHEMA}" IDENTIFIED BY "${ORACLE_SCHEMA_PASSWORD}"';
  ELSE
    EXECUTE IMMEDIATE 'ALTER USER "${ORACLE_SCHEMA}" IDENTIFIED BY "${ORACLE_SCHEMA_PASSWORD}"';
  END IF;
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO "${ORACLE_SCHEMA}"';
END;
/
@${TMP_SQL}
EXIT
SQL
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
        "no Oracle CLI (SQLCL/sql or sqlplus) is installed and python package 'oracledb' is unavailable for Oracle materialization"
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
    if os.environ.get("ORACLE_USER", "sys").lower() == "sys":
        schema = os.environ.get("ORACLE_SCHEMA", "MIGRATIONTEST")
        schema_password = os.environ.get("ORACLE_SCHEMA_PASSWORD", schema.lower())
        cursor.execute(
            """
            DECLARE
                v_count NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_count FROM ALL_USERS WHERE USERNAME = UPPER(:schema_name);
                IF v_count = 0 THEN
                    EXECUTE IMMEDIATE 'CREATE USER "' || :schema_name || '" IDENTIFIED BY "' || :schema_password || '"';
                ELSE
                    EXECUTE IMMEDIATE 'ALTER USER "' || :schema_name || '" IDENTIFIED BY "' || :schema_password || '"';
                END IF;
                EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO "' || :schema_name || '"';
                EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO "' || :schema_name || '"';
                EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO "' || :schema_name || '"';
                EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO "' || :schema_name || '"';
                EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO "' || :schema_name || '"';
            END;
            """,
            schema_name=schema,
            schema_password=schema_password,
        )
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
