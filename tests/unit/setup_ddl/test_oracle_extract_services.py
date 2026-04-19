"""Tests for Oracle extraction service functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_extract_table_columns_uses_char_length_for_oracle_text() -> None:
    from shared.oracle_extract_services import extract_table_columns

    cur = MagicMock()
    cur.description = [
        ("OWNER",),
        ("TABLE_NAME",),
        ("COLUMN_NAME",),
        ("COLUMN_ID",),
        ("DATA_TYPE",),
        ("DATA_LENGTH",),
        ("CHAR_LENGTH",),
        ("DATA_PRECISION",),
        ("DATA_SCALE",),
        ("NULLABLE",),
        ("IDENTITY_COLUMN",),
    ]
    cur.fetchall.return_value = [
        ("SH", "CUSTOMERS", "NAME", 1, "NVARCHAR2", 80, 20, None, None, "Y", "NO"),
        ("SH", "CUSTOMERS", "TOKEN", 2, "RAW", 16, 16, None, None, "N", "NO"),
    ]
    conn = MagicMock()
    conn.cursor.return_value = cur

    result = extract_table_columns(conn, ["SH"])

    assert result[0]["max_length"] == 20
    assert result[1]["max_length"] == 16


def test_extract_object_types_returns_materialized_view_fqns() -> None:
    from shared.oracle_extract_services import extract_object_types

    valid_cur = MagicMock()
    valid_cur.description = [("OWNER",), ("OBJECT_NAME",), ("OBJECT_TYPE",)]
    valid_cur.fetchall.return_value = [
        ("SH", "SALES", "TABLE"),
        ("SH", "PROFITS", "MATERIALIZED VIEW"),
    ]
    invalid_cur = MagicMock()
    invalid_cur.description = [("OWNER",), ("OBJECT_NAME",), ("OBJECT_TYPE",), ("STATUS",)]
    invalid_cur.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.side_effect = [valid_cur, invalid_cur]

    rows, mv_fqns = extract_object_types(conn, ["SH"])

    assert rows == [
        {"schema_name": "SH", "name": "SALES", "type": "U"},
        {"schema_name": "SH", "name": "PROFITS", "type": "V"},
    ]
    assert mv_fqns == ["sh.profits"]


def test_extract_dmf_rejects_unknown_dependency_type() -> None:
    from shared.oracle_extract_services import extract_dmf

    with pytest.raises(ValueError, match="dep_type must be one of"):
        extract_dmf(MagicMock(), ["SH"], "PACKAGE")
