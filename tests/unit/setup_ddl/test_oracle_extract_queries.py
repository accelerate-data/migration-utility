"""Tests for Oracle extraction SQL builders."""

from __future__ import annotations

import pytest


def test_view_text_sql_scopes_uppercase_owners() -> None:
    from shared.oracle_extract_queries import view_text_sql

    sql = view_text_sql(["sh", "hr"])

    assert "FROM ALL_VIEWS" in sql
    assert "OWNER IN ('SH', 'HR')" in sql
    assert "ORDER BY OWNER, VIEW_NAME" in sql


def test_table_columns_sql_scopes_valid_tables_only() -> None:
    from shared.oracle_extract_queries import table_columns_sql

    sql = table_columns_sql(["SH"])

    assert "FROM ALL_TAB_COLUMNS c" in sql
    assert "OBJECT_TYPE = 'TABLE'" in sql
    assert "STATUS = 'VALID'" in sql
    assert "c.OWNER IN ('SH')" in sql
    assert "ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID" in sql


def test_object_type_sql_includes_materialized_views_and_valid_status() -> None:
    from shared.oracle_extract_queries import object_types_sql

    sql = object_types_sql(["SH"])

    assert "'MATERIALIZED VIEW'" in sql
    assert "STATUS = 'VALID'" in sql
    assert "FROM ALL_OBJECTS" in sql


def test_dmf_sql_rejects_unknown_dependency_type() -> None:
    from shared.oracle_extract_queries import dmf_sql

    with pytest.raises(ValueError, match="dep_type must be one of"):
        dmf_sql(["SH"], "PACKAGE")


def test_dmf_sql_maps_dependency_type_into_filter() -> None:
    from shared.oracle_extract_queries import dmf_sql

    sql = dmf_sql(["SH"], "PROCEDURE")

    assert "FROM ALL_DEPENDENCIES" in sql
    assert "WHERE TYPE = 'PROCEDURE'" in sql
    assert "OWNER IN ('SH')" in sql


def test_view_columns_sql_scopes_valid_views() -> None:
    from shared.oracle_extract_queries import view_columns_sql

    sql = view_columns_sql(["SH"])

    assert "FROM ALL_TAB_COLUMNS c" in sql
    assert "OBJECT_TYPE IN ('VIEW', 'MATERIALIZED VIEW')" in sql
    assert "STATUS = 'VALID'" in sql
    assert "c.OWNER IN ('SH')" in sql
    assert "ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID" in sql
