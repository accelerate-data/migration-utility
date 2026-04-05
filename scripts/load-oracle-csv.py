#!/usr/bin/env python3
"""Load Oracle SH sample CSVs that SQLcl LOAD mishandles.

Handles RFC 4180 quoting (commas inside quoted fields) correctly.
Called by setup-oracle.sh after table creation.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import oracledb

DSN = sys.argv[1] if len(sys.argv) > 1 else "localhost:1521/FREEPDB1"
USER = "sh"
PASSWORD = "sh"
CSV_DIR = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(".")

TABLES: dict[str, dict[str, str]] = {
    "customers": {
        "file": "customers.csv",
        "date_cols": {"CUST_EFF_FROM", "CUST_EFF_TO"},
    },
    "supplementary_demographics": {
        "file": "supplementary_demographics.csv",
        "date_cols": set(),
    },
}


def load_table(
    cursor: oracledb.Cursor,
    table: str,
    csv_path: Path,
    date_cols: set[str],
) -> int:
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        if not columns:
            print(f"  SKIP {table}: empty CSV")
            return 0

        placeholders = []
        for col in columns:
            if col in date_cols:
                placeholders.append(f"TO_DATE(:{col}, 'YYYY-MM-DD')")
            else:
                placeholders.append(f":{col}")

        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )

        batch: list[dict[str, str | None]] = []
        total = 0
        for row in reader:
            # Convert empty strings to None
            clean = {k: (v if v else None) for k, v in row.items()}
            batch.append(clean)
            if len(batch) >= 10000:
                cursor.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            cursor.executemany(sql, batch)
            total += len(batch)

        return total


def main() -> None:
    conn = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
    cursor = conn.cursor()

    for table, cfg in TABLES.items():
        csv_path = CSV_DIR / cfg["file"]
        if not csv_path.exists():
            print(f"  SKIP {table}: {csv_path} not found")
            continue

        # Truncate before reload
        cursor.execute(f"DELETE FROM {table}")
        count = load_table(cursor, table, csv_path, cfg["date_cols"])
        conn.commit()
        print(f"  {table}: {count:,} rows loaded")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
