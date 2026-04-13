#!/usr/bin/env bash
set -euo pipefail

: "${DUCKDB_PATH:=.runtime/duckdb/migrationtest.duckdb}"
mkdir -p "$(dirname "${DUCKDB_PATH}")"

python - <<'PY' "${DUCKDB_PATH}"
import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("python package 'duckdb' is required to materialize DuckDB MigrationTest") from exc

db_path = Path(sys.argv[1])
conn = duckdb.connect(str(db_path))
try:
    conn.execute("drop table if exists migrationtest_fixture_info")
    conn.execute(
        "create table migrationtest_fixture_info (fixture_name text not null, backend text not null)"
    )
    conn.execute(
        "insert into migrationtest_fixture_info (fixture_name, backend) values (?, ?)",
        ("MigrationTest", "duckdb"),
    )
    conn.commit()
finally:
    conn.close()
PY

echo "materialize-migration-test duckdb path=${DUCKDB_PATH}"
