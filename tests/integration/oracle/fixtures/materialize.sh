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
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
FIXTURE_SQL="${REPO_ROOT}/tests/integration/oracle/fixtures/migration-test.sql"

if [[ ! -f "${FIXTURE_SQL}" ]]; then
  echo "Oracle fixture SQL file not found: ${FIXTURE_SQL}" >&2
  exit 1
fi

TMP_SQL="$(mktemp)"
TMP_BOOTSTRAP_SQL="$(mktemp)"
trap 'rm -f "${TMP_SQL}" "${TMP_BOOTSTRAP_SQL}"' EXIT
sed "s/__SCHEMA__/${ORACLE_SCHEMA}/g" "${FIXTURE_SQL}" > "${TMP_SQL}"

if [[ "${ORACLE_USER,,}" != "${ORACLE_SCHEMA,,}" ]]; then
  BOOTSTRAP_SQL="$(cat <<SQL
DECLARE
  v_count NUMBER;
BEGIN
  FOR session_rec IN (
    SELECT sid, serial#
    FROM v\$session
    WHERE username = UPPER('${ORACLE_SCHEMA}')
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER SYSTEM KILL SESSION ''' || session_rec.sid || ',' || session_rec.serial# || ''' IMMEDIATE';
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END LOOP;

  SELECT COUNT(*) INTO v_count FROM ALL_USERS WHERE USERNAME = UPPER('${ORACLE_SCHEMA}');
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE
      'CREATE USER "${ORACLE_SCHEMA}" IDENTIFIED BY "${ORACLE_SCHEMA_PASSWORD}" ACCOUNT UNLOCK';
  ELSE
    EXECUTE IMMEDIATE
      'ALTER USER "${ORACLE_SCHEMA}" IDENTIFIED BY "${ORACLE_SCHEMA_PASSWORD}" ACCOUNT UNLOCK';
  END IF;

  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO "${ORACLE_SCHEMA}"';
  EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO "${ORACLE_SCHEMA}"';
END;
/
BEGIN
  FOR object_rec IN (
    SELECT object_name, object_type
    FROM all_objects
    WHERE owner = UPPER('${ORACLE_SCHEMA}')
      AND object_type IN (
        'VIEW',
        'MATERIALIZED VIEW',
        'SYNONYM',
        'PROCEDURE',
        'FUNCTION',
        'PACKAGE',
        'TABLE',
        'SEQUENCE'
      )
      AND NOT (object_type = 'SEQUENCE' AND generated = 'Y')
    ORDER BY CASE object_type
      WHEN 'VIEW' THEN 1
      WHEN 'MATERIALIZED VIEW' THEN 2
      WHEN 'SYNONYM' THEN 3
      WHEN 'PROCEDURE' THEN 4
      WHEN 'FUNCTION' THEN 5
      WHEN 'PACKAGE' THEN 6
      WHEN 'TABLE' THEN 7
      WHEN 'SEQUENCE' THEN 8
      ELSE 9
    END
  ) LOOP
    BEGIN
      IF object_rec.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE
          'DROP TABLE "${ORACLE_SCHEMA}"."' || object_rec.object_name || '" CASCADE CONSTRAINTS PURGE';
      ELSIF object_rec.object_type = 'MATERIALIZED VIEW' THEN
        EXECUTE IMMEDIATE
          'DROP MATERIALIZED VIEW "${ORACLE_SCHEMA}"."' || object_rec.object_name || '"';
      ELSE
        EXECUTE IMMEDIATE
          'DROP ' || object_rec.object_type || ' "${ORACLE_SCHEMA}"."' || object_rec.object_name || '"';
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE NOT IN (-942, -4043) THEN
          RAISE;
        END IF;
    END;
  END LOOP;
END;
/
SQL
)"
else
  BOOTSTRAP_SQL="$(cat <<SQL
BEGIN
  FOR object_rec IN (
    SELECT object_name, object_type
    FROM user_objects
    WHERE object_type IN (
      'VIEW',
      'MATERIALIZED VIEW',
      'SYNONYM',
      'PROCEDURE',
      'FUNCTION',
      'PACKAGE',
      'TABLE',
      'SEQUENCE'
    )
      AND NOT (object_type = 'SEQUENCE' AND generated = 'Y')
    ORDER BY CASE object_type
      WHEN 'VIEW' THEN 1
      WHEN 'MATERIALIZED VIEW' THEN 2
      WHEN 'SYNONYM' THEN 3
      WHEN 'PROCEDURE' THEN 4
      WHEN 'FUNCTION' THEN 5
      WHEN 'PACKAGE' THEN 6
      WHEN 'TABLE' THEN 7
      WHEN 'SEQUENCE' THEN 8
      ELSE 9
    END
  ) LOOP
    BEGIN
      IF object_rec.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE
          'DROP TABLE "' || object_rec.object_name || '" CASCADE CONSTRAINTS PURGE';
      ELSIF object_rec.object_type = 'MATERIALIZED VIEW' THEN
        EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW "' || object_rec.object_name || '"';
      ELSE
        EXECUTE IMMEDIATE
          'DROP ' || object_rec.object_type || ' "' || object_rec.object_name || '"';
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE NOT IN (-942, -4043) THEN
          RAISE;
        END IF;
    END;
  END LOOP;
END;
/
SQL
)"
fi

printf '%s\n' "${BOOTSTRAP_SQL}" > "${TMP_BOOTSTRAP_SQL}"

echo "materialize-migration-test oracle service=${ORACLE_SERVICE} host=${ORACLE_HOST} port=${ORACLE_PORT} schema=${ORACLE_SCHEMA}"
run_python_materialization() {
python - <<'PY' "${TMP_BOOTSTRAP_SQL}" "${TMP_SQL}"
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

bootstrap_path = Path(sys.argv[1])
fixture_path = Path(sys.argv[2])
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
    def execute_script(path: Path) -> None:
        raw = path.read_text(encoding="utf-8")
        raw = "\n".join(
            line for line in raw.splitlines()
            if not line.lstrip().upper().startswith("WHENEVER ")
        )
        chunks = re.split(r"(?m)^[ \t]*/[ \t]*$", raw)
        for chunk in chunks:
            statement = chunk.strip()
            if statement:
                cursor.execute(statement)

    execute_script(bootstrap_path)
    execute_script(fixture_path)
    conn.commit()
finally:
    conn.close()
PY
}

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
  if "${ORACLE_CLI}" -S /nolog <<SQL
${CONNECT_DIRECTIVE}
@${TMP_BOOTSTRAP_SQL}
@${TMP_SQL}
EXIT
SQL
  then
    exit 0
  fi
  echo "Oracle CLI materialization failed; retrying with python oracledb fallback" >&2
fi

run_python_materialization
