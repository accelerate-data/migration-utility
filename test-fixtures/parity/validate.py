#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyodbc>=5.0",
#   "oracledb>=2.0",
#   "psycopg2-binary>=2.9",
# ]
# ///
"""Cross-dialect parity validation for Kimball fixture.

Runs usp_exec_orchestrator_full_load on SQL Server, Oracle, and PostgreSQL,
compares all 20 output tables after baseline load and after each of 5 delta
scenarios. SQL Server is the reference dialect.

Usage:
    uv run test-fixtures/parity/validate.py [--table TABLE]

Options:
    --table TABLE   Validate a single table only (default: all 20)

Exit code 0 = all rounds pass. Exit code 1 = any table mismatch or error.
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import json
import re
import sys
from pathlib import Path

import oracledb
import psycopg2
import pyodbc

# ---------------------------------------------------------------------------
# Connection constants
# ---------------------------------------------------------------------------

_SS_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=KimballFixture;"
    "UID=sa;PWD=P@ssw0rd123;"
    "TrustServerCertificate=yes;"
)

_ORA_CONN = dict(user="kimball", password="kimball", dsn="localhost:1521/FREEPDB1")

_PG_CONN = dict(host="localhost", port=5432, dbname="kimball_fixture", user="postgres", password="postgres")

# ---------------------------------------------------------------------------
# Output tables: (sql_server_qualified, oracle_flat, postgres_qualified)
# Sorted roughly in load order; static reference tables (dim_date,
# dim_order_status) are excluded — they are not written by procedures.
# ---------------------------------------------------------------------------

OUTPUT_TABLES: list[tuple[str, str, str]] = [
    ("dim.dim_product_category", "dim_product_category", "dim.dim_product_category"),
    ("dim.dim_product", "dim_product", "dim.dim_product"),
    ("dim.dim_customer", "dim_customer", "dim.dim_customer"),
    ("dim.dim_employee", "dim_employee", "dim.dim_employee"),
    ("dim.dim_address", "dim_address", "dim.dim_address"),
    ("dim.dim_credit_card", "dim_credit_card", "dim.dim_credit_card"),
    ("fact.fct_sales", "fct_sales", "fact.fct_sales"),
    ("fact.fct_sales_summary", "fct_sales_summary", "fact.fct_sales_summary"),
    ("fact.fct_sales_by_channel", "fct_sales_by_channel", "fact.fct_sales_by_channel"),
    ("gold.rpt_customer_lifetime_value", "rpt_customer_lifetime_value", "gold.rpt_customer_lifetime_value"),
    ("gold.rpt_product_performance", "rpt_product_performance", "gold.rpt_product_performance"),
    ("gold.rpt_sales_by_territory", "rpt_sales_by_territory", "gold.rpt_sales_by_territory"),
    ("gold.rpt_employee_hierarchy", "rpt_employee_hierarchy", "gold.rpt_employee_hierarchy"),
    ("gold.rpt_sales_by_category", "rpt_sales_by_category", "gold.rpt_sales_by_category"),
    ("gold.rpt_channel_pivot", "rpt_channel_pivot", "gold.rpt_channel_pivot"),
    ("gold.rpt_returns_analysis", "rpt_returns_analysis", "gold.rpt_returns_analysis"),
    ("gold.rpt_customer_segments", "rpt_customer_segments", "gold.rpt_customer_segments"),
    ("gold.rpt_address_coverage", "rpt_address_coverage", "gold.rpt_address_coverage"),
    ("gold.rpt_product_margin", "rpt_product_margin", "gold.rpt_product_margin"),
    ("gold.rpt_date_sales_rollup", "rpt_date_sales_rollup", "gold.rpt_date_sales_rollup"),
]

# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_DECIMAL_PLACES = 2


def _normalize_value(v: object) -> object:
    """Normalize a single cell value for cross-dialect comparison."""
    if v is None:
        return "__NULL__"
    if isinstance(v, bool):
        # PostgreSQL BOOLEAN → int to match SQL Server BIT / Oracle NUMBER(1)
        return int(v)
    if isinstance(v, (datetime.datetime, datetime.date)):
        if isinstance(v, datetime.datetime):
            return v.date().isoformat()
        return v.isoformat()
    if isinstance(v, (float, decimal.Decimal)):
        return round(float(v), _DECIMAL_PLACES)
    return v


def _normalize_row(row: dict[str, object]) -> dict[str, object]:
    """Lowercase column names and normalize all values."""
    return {k.lower(): _normalize_value(v) for k, v in row.items()}


def _normalize_rows(rows: list[dict[str, object]]) -> list[str]:
    """Normalize and sort rows; return JSON strings for set comparison."""
    return sorted(
        json.dumps(_normalize_row(r), sort_keys=True, default=str) for r in rows
    )


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _connect_ss() -> pyodbc.Connection:
    conn = pyodbc.connect(_SS_CONN_STR, timeout=30)
    conn.autocommit = True
    return conn


def _connect_ora() -> oracledb.Connection:
    conn = oracledb.connect(**_ORA_CONN)
    conn.autocommit = True
    return conn


def _connect_pg() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(**_PG_CONN)
    conn.autocommit = True
    return conn


# ---------------------------------------------------------------------------
# Row fetching
# ---------------------------------------------------------------------------


def _cursor_to_dicts(cursor: object) -> list[dict[str, object]]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _fetch_ss(conn: pyodbc.Connection, table: str) -> list[dict[str, object]]:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")  # noqa: S608 — table names from constant list
    return _cursor_to_dicts(cur)


def _fetch_ora(conn: oracledb.Connection, table: str) -> list[dict[str, object]]:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")  # noqa: S608
    return _cursor_to_dicts(cur)


def _fetch_pg(conn: psycopg2.extensions.connection, table: str) -> list[dict[str, object]]:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")  # noqa: S608
    return _cursor_to_dicts(cur)


# ---------------------------------------------------------------------------
# Orchestrator execution
# ---------------------------------------------------------------------------


def _run_orchestrator_ss(conn: pyodbc.Connection) -> None:
    cur = conn.cursor()
    cur.execute("{CALL dbo.usp_exec_orchestrator_full_load}")
    # Drain all result sets (procs may produce intermediate rowsets)
    while cur.nextset():
        pass


def _run_orchestrator_ora(conn: oracledb.Connection) -> None:
    cur = conn.cursor()
    cur.execute("BEGIN usp_exec_orchestrator_full_load(); END;")


def _run_orchestrator_pg(conn: psycopg2.extensions.connection) -> None:
    cur = conn.cursor()
    cur.execute("CALL public.usp_exec_orchestrator_full_load()")


# ---------------------------------------------------------------------------
# Delta SQL application
# ---------------------------------------------------------------------------


def _apply_delta_ss(conn: pyodbc.Connection, sql_path: Path) -> None:
    """Execute a SQL Server delta file, splitting on GO boundaries."""
    text = sql_path.read_text(encoding="utf-8")
    batches = re.split(r"^\s*GO\s*$", text, flags=re.IGNORECASE | re.MULTILINE)
    cur = conn.cursor()
    for batch in batches:
        batch = batch.strip()
        if not batch or re.match(r"^\s*USE\b", batch, re.IGNORECASE):
            continue
        cur.execute(batch)


def _apply_delta_ora(conn: oracledb.Connection, sql_path: Path) -> None:
    """Execute an Oracle delta file, splitting on semicolons."""
    text = sql_path.read_text(encoding="utf-8")
    cur = conn.cursor()
    for stmt in text.split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.upper() in {"COMMIT", "--"}:
            continue
        # Skip comment-only lines
        if all(line.strip().startswith("--") for line in stmt.splitlines() if line.strip()):
            continue
        cur.execute(stmt)
    conn.commit()


def _apply_delta_pg(conn: psycopg2.extensions.connection, sql_path: Path) -> None:
    """Execute a PostgreSQL delta file, splitting on semicolons."""
    text = sql_path.read_text(encoding="utf-8")
    cur = conn.cursor()
    for stmt in text.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        if all(line.strip().startswith("--") for line in stmt.splitlines() if line.strip()):
            continue
        cur.execute(stmt)


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------


def _diff(
    ref: list[str],
    cmp: list[str],
    label: str,
) -> tuple[bool, str]:
    """Compare two normalized row lists (as sorted JSON strings).

    Returns (match: bool, message: str).
    """
    ref_set = set(ref)
    cmp_set = set(cmp)
    only_ref = sorted(ref_set - cmp_set)
    only_cmp = sorted(cmp_set - ref_set)
    if not only_ref and not only_cmp:
        return True, f"{label}: PASS  ref={len(ref)} {len(cmp)}"
    lines = [
        f"{label}: FAIL  ref={len(ref)} cmp={len(cmp)}"
        f"  diff_ref={len(only_ref)} diff_cmp={len(only_cmp)}",
    ]
    if only_ref:
        lines.append("  SQL Server-only rows (first 5):")
        for r in only_ref[:5]:
            lines.append(f"    {r}")
    if only_cmp:
        lines.append(f"  {label.split()[-1]}-only rows (first 5):")
        for r in only_cmp[:5]:
            lines.append(f"    {r}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Single round: run orchestrator, compare all tables
# ---------------------------------------------------------------------------


def _run_round(
    round_label: str,
    ss_conn: pyodbc.Connection,
    ora_conn: oracledb.Connection,
    pg_conn: psycopg2.extensions.connection,
    tables: list[tuple[str, str, str]],
) -> bool:
    """Run orchestrator on all dialects and compare all output tables."""
    print(f"\n{'='*60}")
    print(f"Round: {round_label}")
    print(f"{'='*60}")

    print("  Running orchestrator — SQL Server ... ", end="", flush=True)
    _run_orchestrator_ss(ss_conn)
    print("done")

    print("  Running orchestrator — Oracle ...     ", end="", flush=True)
    _run_orchestrator_ora(ora_conn)
    print("done")

    print("  Running orchestrator — PostgreSQL ... ", end="", flush=True)
    _run_orchestrator_pg(pg_conn)
    print("done\n")

    round_ok = True
    for ss_table, ora_table, pg_table in tables:
        ss_rows = _normalize_rows(_fetch_ss(ss_conn, ss_table))
        ora_rows = _normalize_rows(_fetch_ora(ora_conn, ora_table))
        pg_rows = _normalize_rows(_fetch_pg(pg_conn, pg_table))

        ora_ok, ora_msg = _diff(ss_rows, ora_rows, f"[{round_label}] {ss_table} vs oracle")
        pg_ok, pg_msg = _diff(ss_rows, pg_rows, f"[{round_label}] {ss_table} vs pg")

        if ora_ok and pg_ok:
            print(f"  PASS  {ss_table}  ref={len(ss_rows)}")
        else:
            print(f"  {ora_msg}")
            print(f"  {pg_msg}")
            round_ok = False

    return round_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_DELTA_DIRS_SORTED = sorted(
    (Path(__file__).parent.parent / "data" / "delta").iterdir()
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--table",
        metavar="TABLE",
        help="Validate a single SQL Server-qualified table only (e.g. dim.dim_customer)",
    )
    args = parser.parse_args()

    tables = OUTPUT_TABLES
    if args.table:
        tables = [t for t in OUTPUT_TABLES if t[0] == args.table]
        if not tables:
            valid = ", ".join(t[0] for t in OUTPUT_TABLES)
            print(f"ERROR: unknown table {args.table!r}. Valid tables:\n  {valid}", file=sys.stderr)
            return 1

    print("Connecting to all three dialect containers...")
    try:
        ss_conn = _connect_ss()
    except Exception as exc:
        print(f"ERROR connecting to SQL Server (sql-test): {exc}", file=sys.stderr)
        return 1
    try:
        ora_conn = _connect_ora()
    except Exception as exc:
        print(f"ERROR connecting to Oracle (oracle-test): {exc}", file=sys.stderr)
        return 1
    try:
        pg_conn = _connect_pg()
    except Exception as exc:
        print(f"ERROR connecting to PostgreSQL (pg-test): {exc}", file=sys.stderr)
        return 1
    print("Connected.\n")

    all_ok = True

    # Baseline round
    ok = _run_round("Baseline", ss_conn, ora_conn, pg_conn, tables)
    all_ok = all_ok and ok

    # Delta rounds
    for delta_dir in _DELTA_DIRS_SORTED:
        if not delta_dir.is_dir():
            continue
        ss_sql = delta_dir / "sqlserver.sql"
        ora_sql = delta_dir / "oracle.sql"
        pg_sql = delta_dir / "postgres.sql"
        if not (ss_sql.exists() and ora_sql.exists() and pg_sql.exists()):
            print(f"  SKIP {delta_dir.name}: missing dialect SQL files", file=sys.stderr)
            continue

        label = delta_dir.name
        print(f"\nApplying delta: {label}")
        _apply_delta_ss(ss_conn, ss_sql)
        _apply_delta_ora(ora_conn, ora_sql)
        _apply_delta_pg(pg_conn, pg_sql)

        ok = _run_round(label, ss_conn, ora_conn, pg_conn, tables)
        all_ok = all_ok and ok

    print(f"\n{'='*60}")
    if all_ok:
        print("ALL ROUNDS PASSED")
    else:
        print("SOME ROUNDS FAILED — see details above")
    print(f"{'='*60}")

    ss_conn.close()
    ora_conn.close()
    pg_conn.close()

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
