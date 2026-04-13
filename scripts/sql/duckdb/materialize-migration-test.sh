#!/usr/bin/env bash
set -euo pipefail

: "${DUCKDB_PATH:=.runtime/duckdb/migrationtest.duckdb}"
mkdir -p "$(dirname "${DUCKDB_PATH}")"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SQL_FILE="${REPO_ROOT}/scripts/sql/duckdb/migration_test.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "DuckDB fixture SQL file not found: ${SQL_FILE}" >&2
  exit 1
fi

python - <<'PY' "${DUCKDB_PATH}" "${SQL_FILE}"
import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("python package 'duckdb' is required to materialize DuckDB MigrationTest") from exc

db_path = Path(sys.argv[1])
sql_path = Path(sys.argv[2])
conn = duckdb.connect(str(db_path))
try:
    conn.execute(sql_path.read_text(encoding="utf-8"))
    conn.commit()
finally:
    conn.close()
PY

echo "materialize-migration-test duckdb path=${DUCKDB_PATH}"
