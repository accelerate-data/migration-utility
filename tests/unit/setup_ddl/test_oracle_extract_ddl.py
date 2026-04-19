"""Tests for Oracle extraction DDL helpers."""

from __future__ import annotations

from unittest.mock import MagicMock


def test_definition_from_view_text_uses_clean_create_view_shape() -> None:
    from shared.oracle_extract_ddl import definition_from_view_text

    definition = definition_from_view_text("SH", "PROFITS", "select 1 from dual")

    assert definition == "CREATE OR REPLACE VIEW SH.PROFITS AS\nselect 1 from dual"


def test_extract_view_ddl_rows_falls_back_for_truncated_long_text() -> None:
    from shared.oracle_extract_ddl import extract_view_ddl_rows

    fallback_ddl = "CREATE OR REPLACE VIEW SH.PROFITS AS SELECT 1 FROM DUAL"
    truncated_text = "x" * 32767
    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
    main_cur.fetchall.return_value = [("SH", "PROFITS", truncated_text)]
    ddl_cur = MagicMock()
    clob = MagicMock()
    clob.read.return_value = fallback_ddl
    ddl_cur.fetchone.return_value = (clob,)
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_view_ddl_rows(conn, ["SH"])

    assert result == [{"schema_name": "SH", "object_name": "PROFITS", "definition": fallback_ddl}]
    mock_sql = ddl_cur.execute.call_args.args[0]
    mock_binds = ddl_cur.execute.call_args.kwargs
    assert "GET_DDL('VIEW', :n, :o)" in mock_sql
    assert mock_binds == {"n": "PROFITS", "o": "SH"}


def test_extract_definition_rows_skips_when_metadata_fetch_returns_no_row() -> None:
    from shared.oracle_extract_ddl import extract_definition_rows

    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("OBJECT_NAME",), ("OBJECT_TYPE",)]
    main_cur.fetchall.return_value = [("SH", "LOAD_SALES", "PROCEDURE")]
    ddl_cur = MagicMock()
    ddl_cur.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_definition_rows(conn, ["SH"])

    assert result == []


def test_extract_view_ddl_rows_skips_empty_text_when_metadata_fetch_returns_no_row() -> None:
    from shared.oracle_extract_ddl import extract_view_ddl_rows

    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
    main_cur.fetchall.return_value = [("SH", "PROFITS", "")]
    ddl_cur = MagicMock()
    ddl_cur.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_view_ddl_rows(conn, ["SH"])

    assert result == []


def test_extract_view_ddl_rows_keeps_truncated_diagnostic_when_metadata_fails() -> None:
    from shared.oracle_extract_ddl import extract_view_ddl_rows

    truncated_text = "x" * 32767
    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
    main_cur.fetchall.return_value = [("SH", "PROFITS", truncated_text)]
    ddl_cur = MagicMock()
    ddl_cur.execute.side_effect = RuntimeError("metadata denied")
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_view_ddl_rows(conn, ["SH"])

    assert result == [
        {
            "schema_name": "SH",
            "object_name": "PROFITS",
            "definition": f"CREATE OR REPLACE VIEW SH.PROFITS AS\n{truncated_text}",
            "long_truncation": True,
        }
    ]
