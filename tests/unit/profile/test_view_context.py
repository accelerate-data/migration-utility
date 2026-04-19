from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import profile
from shared.loader import CatalogFileMissingError
from shared.output_models.profile import ViewProfileContext
from tests.unit.profile.helpers import _PROFILE_FIXTURES, _make_writable_copy

_cli_runner = CliRunner()


def test_view_context_object_types() -> None:
    """object_type is stamped correctly on all reference buckets."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")
    assert isinstance(result, ViewProfileContext)

    # references.tables in_scope → "table"
    for entry in result.references.tables.in_scope:
        assert entry.object_type == "table"
    # references.views in_scope → "view"
    for entry in result.references.views.in_scope:
        assert entry.object_type == "view"
    # references.functions in_scope → "function"
    for entry in result.references.functions.in_scope:
        assert entry.object_type == "function"
    # referenced_by.procedures in_scope → "procedure"
    for entry in result.referenced_by.procedures.in_scope:
        assert entry.object_type == "procedure"

def test_view_context_multi_sql_elements() -> None:
    """sql_elements and logic_summary are surfaced from scoping."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")
    assert isinstance(result, ViewProfileContext)
    element_types = {e.type for e in result.sql_elements}
    assert "join" in element_types
    assert "aggregation" in element_types
    assert "group_by" in element_types
    assert "Joins FactSales" in result.logic_summary

def test_view_context_mv_includes_columns() -> None:
    """Materialized views surface columns; is_materialized_view is True."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.mv_Monthly")
    assert isinstance(result, ViewProfileContext)
    assert result.is_materialized_view is True
    col_names = [c.name for c in result.columns]
    assert "month_key" in col_names
    assert "total_amount" in col_names

def test_view_context_mv_columns_expose_only_target_sql_type() -> None:
    """Materialized view context hides source/debug/legacy type fields."""
    tmp, root = _make_writable_copy()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.mv_monthly.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["columns"][0].update(
            {
                "type": "NUMBER",
                "data_type": "NUMBER(10,0)",
                "source_sql_type": "NUMBER(10,0)",
                "canonical_tsql_type": "INT",
                "sql_type": "INT",
            }
        )
        view_path.write_text(json.dumps(cat), encoding="utf-8")

        result = profile.run_view_context(root, "silver.mv_Monthly")

    column = result.columns[0].model_dump(exclude_none=True)
    assert column["sql_type"] == "INT"
    assert "source_sql_type" not in column
    assert "canonical_tsql_type" not in column
    assert "data_type" not in column
    assert "type" not in column

def test_view_context_non_mv_no_columns() -> None:
    """Non-materialized view returns empty columns list."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Simple")
    assert result.columns == []

def test_view_context_missing_catalog_raises() -> None:
    """Missing view catalog raises CatalogFileMissingError."""
    with pytest.raises(CatalogFileMissingError):
        profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_NoExist")

def test_view_context_no_scoping_raises() -> None:
    """View catalog without scoping section raises ValueError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_noscope.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_NoScope",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="scoping not completed"):
            profile.run_view_context(root, "silver.vw_NoScope")

def test_view_context_scoping_error_status_raises() -> None:
    """View with scoping.status=error raises ValueError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_err.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_Err",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "scoping": {"status": "error", "sql_elements": None, "warnings": [], "errors": []},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="scoping not completed"):
            profile.run_view_context(root, "silver.vw_Err")


def test_profile_support_exports_view_context() -> None:
    from shared.profile_support.view_context import run_view_context

    result = run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")

    assert result.view == "silver.vw_multi"
    assert result.references.tables.in_scope
