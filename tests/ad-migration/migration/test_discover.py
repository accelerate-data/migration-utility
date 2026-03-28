"""Tests for discover.py — DDL object catalog CLI.

Tests import shared.discover core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  Run via uv to ensure shared is
importable: uv run --project <shared> pytest tests/ad-migration/migration/
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import DdlParseError

_TESTS_DIR = Path(__file__).parent
_FLAT_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "flat"
_UNPARSEABLE_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "unparseable"


# ── test_list_flat_tables ──────────────────────────────────────────────────


def test_list_flat_tables() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
    objects = result["objects"]
    assert len(objects) == 3
    assert "silver.dimproduct" in objects
    assert "bronze.product" in objects
    assert "dbo.config" in objects


# ── test_list_flat_procedures ─────────────────────────────────────────────


def test_list_flat_procedures() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures, "tsql")
    objects = result["objects"]
    assert len(objects) == 2
    assert "dbo.usp_loaddimproduct" in objects
    assert "dbo.usp_logmessage" in objects


# ── test_list_flat_missing_optional ───────────────────────────────────────


def test_list_flat_missing_optional() -> None:
    """Directory with only tables.sql — views list returns empty without error."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(
            "CREATE TABLE dbo.SomeTable (Id INT)\nGO\n", encoding="utf-8"
        )
        result = discover.run_list(p, discover.ObjectType.views, "tsql")
    assert result["objects"] == []


# ── test_list_indexed_same_as_flat ────────────────────────────────────────


def test_list_indexed_same_as_flat() -> None:
    """Indexed dir returns same object names as flat dir."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "indexed"
        index_directory(_FLAT_FIXTURES, out)

        flat_result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
        indexed_result = discover.run_list(out, discover.ObjectType.tables, "tsql")

    assert flat_result["objects"] == indexed_result["objects"]


# ── test_list_unparseable_raises ──────────────────────────────────────────


def test_list_unparseable_raises() -> None:
    """Loading a dir with unparseable DDL raises DdlParseError."""
    with pytest.raises(DdlParseError, match="Command"):
        discover.run_list(_UNPARSEABLE_FIXTURES, discover.ObjectType.procedures, "tsql")


# ── test_show_table_columns ───────────────────────────────────────────────


def test_show_table_columns() -> None:
    """show on a table returns columns list populated from AST."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    assert result["type"] == "table"
    assert result["parse_error"] is None
    columns = result["columns"]
    assert isinstance(columns, list)
    col_names = [c["name"] for c in columns]
    assert "ProductKey" in col_names
    assert "ProductAlternateKey" in col_names
    assert "EnglishProductName" in col_names
    # Every column entry has name and sql_type keys
    for col in columns:
        assert "name" in col
        assert "sql_type" in col


# ── test_show_unparseable_raises ──────────────────────────────────────────


def test_show_unparseable_raises() -> None:
    """show on a dir with unparseable DDL raises DdlParseError."""
    with pytest.raises(DdlParseError, match="Command"):
        discover.run_show(_UNPARSEABLE_FIXTURES, "dbo.usp_ConditionalLoad", "tsql")


# ── test_refs_ast_bracket_notation ────────────────────────────────────────


def test_refs_ast_bracket_notation() -> None:
    """refs for silver.DimProduct finds proc that uses [silver].[DimProduct] bracket notation."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    assert result["name"] == "silver.dimproduct"
    referenced_by = result["referenced_by"]
    # usp_loaddimproduct uses [silver].[DimProduct] — must be found via AST
    assert "dbo.usp_loaddimproduct" in referenced_by


# ── test_refs_no_false_positive ───────────────────────────────────────────


def test_refs_no_false_positive_string_literal() -> None:
    """Proc that mentions 'silver.DimProduct' only in a comment is NOT returned by refs."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    referenced_by = result["referenced_by"]
    # usp_logmessage only mentions silver.DimProduct in a comment, not in DML
    assert "dbo.usp_logmessage" not in referenced_by


# ── test_discover_cli_exits_on_parse_error ──────────────────────────────


def test_discover_cli_exits_on_parse_error() -> None:
    """discover CLI list command exits code 2 when DDL is unparseable."""
    from typer.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(
        discover.app,
        ["list", "--ddl-path", str(_UNPARSEABLE_FIXTURES), "--type", "procedures"],
    )
    assert result.exit_code == 2
