"""Tests for discover.py — DDL object catalog CLI.

Covers all VU-735 acceptance criteria:

1.  test_list_flat_tables               — flat dir, correct table count + FQNs
2.  test_list_flat_procedures           — flat dir, correct procedure count
3.  test_list_flat_missing_optional     — dir with only tables.sql; views list empty
4.  test_list_indexed_same_as_flat      — indexed dir returns same objects
5.  test_list_unparseable_skipped       — parse-error proc included by FQN, no abort
6.  test_show_table_columns             — show on table → columns from AST
7.  test_show_procedure_parse_error     — show on IF/ELSE proc → parse_error set, lists empty
8.  test_refs_ast_bracket_notation      — refs finds bracket-notation proc reference
9.  test_refs_no_false_positive         — string-literal mention NOT returned by refs

Tests import shared.discover core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  sys.path insertion ensures shared is
importable when running pytest without a prior editable install.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

# ── Path setup ────────────────────────────────────────────────────────────────

_TESTS_DIR = Path(__file__).parent
_SHARED_DIR = _TESTS_DIR.parent

# Make shared importable (editable install covers this in CI; direct pytest run needs it)
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

from shared import discover  # noqa: E402

_FLAT_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "flat"


# ── 1: test_list_flat_tables ──────────────────────────────────────────────────


def test_list_flat_tables() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
    objects = result["objects"]
    assert len(objects) == 3
    assert "silver.dimproduct" in objects
    assert "bronze.product" in objects
    assert "dbo.config" in objects


# ── 2: test_list_flat_procedures ─────────────────────────────────────────────


def test_list_flat_procedures() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures, "tsql")
    objects = result["objects"]
    # Three procs defined in fixtures/discover/flat/procedures.sql
    assert len(objects) == 3
    assert "dbo.usp_loaddimproduct" in objects
    assert "dbo.usp_conditionalload" in objects
    assert "dbo.usp_logmessage" in objects


# ── 3: test_list_flat_missing_optional ───────────────────────────────────────


def test_list_flat_missing_optional() -> None:
    """Directory with only tables.sql — views list returns empty without error."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(
            "CREATE TABLE dbo.SomeTable (Id INT)\nGO\n", encoding="utf-8"
        )
        result = discover.run_list(p, discover.ObjectType.views, "tsql")
    assert result["objects"] == []


# ── 4: test_list_indexed_same_as_flat ────────────────────────────────────────


def test_list_indexed_same_as_flat() -> None:
    """Indexed dir returns same object names as flat dir."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "indexed"
        index_directory(_FLAT_FIXTURES, out)

        flat_result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
        indexed_result = discover.run_list(out, discover.ObjectType.tables, "tsql")

    assert flat_result["objects"] == indexed_result["objects"]


# ── 5: test_list_unparseable_skipped ─────────────────────────────────────────


def test_list_unparseable_skipped() -> None:
    """Parse-error procs are still listed by FQN; load does not abort."""
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures, "tsql")
    objects = result["objects"]
    # usp_conditionalload has IF/ELSE → parse_error, but must still appear in list
    assert "dbo.usp_conditionalload" in objects
    # usp_loaddimproduct parses fine and must also appear
    assert "dbo.usp_loaddimproduct" in objects


# ── 6: test_show_table_columns ───────────────────────────────────────────────


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


# ── 7: test_show_procedure_parse_error ───────────────────────────────────────


def test_show_procedure_parse_error() -> None:
    """show on IF/ELSE proc → parse_error set, params and refs are empty/null."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ConditionalLoad", "tsql")
    assert result["type"] == "procedure"
    assert result["parse_error"] is not None
    assert result["params"] == []
    # refs should be None (cannot be extracted when parse failed)
    assert result["refs"] is None


# ── 8: test_refs_ast_bracket_notation ────────────────────────────────────────


def test_refs_ast_bracket_notation() -> None:
    """refs for silver.DimProduct finds proc that uses [silver].[DimProduct] bracket notation."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    assert result["name"] == "silver.dimproduct"
    referenced_by = result["referenced_by"]
    # usp_loaddimproduct uses [silver].[DimProduct] — must be found via AST
    assert "dbo.usp_loaddimproduct" in referenced_by


# ── 9: test_refs_no_false_positive ───────────────────────────────────────────


def test_refs_no_false_positive_string_literal() -> None:
    """Proc that mentions 'silver.DimProduct' only in a comment is NOT returned by refs."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    referenced_by = result["referenced_by"]
    # usp_logmessage only mentions silver.DimProduct in a comment, not in DML
    assert "dbo.usp_logmessage" not in referenced_by
