#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:=localhost}"
: "${ORACLE_PORT:=1521}"
: "${ORACLE_SERVICE:=FREEPDB1}"
: "${ORACLE_USER:=kimball}"
: "${ORACLE_SCHEMA:=${ORACLE_USER}}"

if [[ -z "${ORACLE_PWD:-}" ]]; then
  echo "ORACLE_PWD must be set" >&2
  exit 1
fi

if ! command -v sqlplus >/dev/null 2>&1; then
  echo "sqlplus is required to materialize MigrationTest for Oracle" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SCHEMA_SQL="${REPO_ROOT}/test-fixtures/schema/oracle.sql"
PROCS_SQL="${REPO_ROOT}/test-fixtures/procedures/oracle.sql"

if [[ ! -f "${SCHEMA_SQL}" || ! -f "${PROCS_SQL}" ]]; then
  echo "Oracle fixture SQL files not found under test-fixtures/" >&2
  exit 1
fi

CONNECT_STRING="${ORACLE_USER}/${ORACLE_PWD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}"

echo "materialize-migration-test oracle service=${ORACLE_SERVICE} host=${ORACLE_HOST} port=${ORACLE_PORT}"
sqlplus -S "${CONNECT_STRING}" @"${SCHEMA_SQL}"
sqlplus -S "${CONNECT_STRING}" @"${PROCS_SQL}"
