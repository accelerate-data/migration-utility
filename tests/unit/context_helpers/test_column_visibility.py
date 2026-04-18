from __future__ import annotations

import json
from pathlib import Path

from shared.context_helpers import load_object_columns, load_table_columns
from shared.discover_support.browse import _show_table


def _write_table(project_root: Path) -> None:
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.customer.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "customer",
                "columns": [
                    {
                        "name": "CUSTOMER_ID",
                        "source_sql_type": "NUMBER(10,0)",
                        "canonical_tsql_type": "INT",
                        "sql_type": "INT",
                        "is_nullable": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_table_column_context_exposes_only_target_sql_type(tmp_path: Path) -> None:
    _write_table(tmp_path)

    assert load_table_columns(tmp_path, "silver.customer") == [
        {"name": "CUSTOMER_ID", "sql_type": "INT", "is_nullable": False}
    ]


def test_object_column_context_exposes_only_target_sql_type(tmp_path: Path) -> None:
    _write_table(tmp_path)

    assert load_object_columns(tmp_path, "silver.customer") == [
        {"name": "CUSTOMER_ID", "sql_type": "INT", "is_nullable": False}
    ]


def test_discover_table_show_exposes_only_target_sql_type(tmp_path: Path) -> None:
    _write_table(tmp_path)

    assert _show_table(tmp_path, "silver.customer")["columns"] == [
        {"name": "CUSTOMER_ID", "sql_type": "INT", "is_nullable": False}
    ]
