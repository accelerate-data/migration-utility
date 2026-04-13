#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:=localhost}"
: "${ORACLE_PORT:=1521}"
: "${ORACLE_SERVICE:=FREEPDB1}"

echo "materialize-migration-test oracle service=${ORACLE_SERVICE} host=${ORACLE_HOST} port=${ORACLE_PORT}"
