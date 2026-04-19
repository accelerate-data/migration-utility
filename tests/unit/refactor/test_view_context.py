from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from shared import refactor
from shared.output_models.refactor import RefactorContextOutput
from tests.unit.refactor.helpers import (
    _REFACTOR_FIXTURES,
    _make_writable_copy,
)

_cli_runner = CliRunner()


def test_context_view_auto_detect() -> None:
    """run_context auto-detects a view FQN and returns view-specific fields."""
    result = refactor.run_context(_REFACTOR_FIXTURES, "silver.vw_active_customers")
    assert isinstance(result, RefactorContextOutput)
    assert result.table == "silver.vw_active_customers"
    assert result.object_type == "view"
    assert result.view_sql is not None
    assert "CustomerID" in result.view_sql
    assert result.writer is None
    assert result.proc_body is None
    assert result.statements is None
    assert result.profile["status"] == "ok"
    assert result.columns[0]["name"] == "CustomerID"
    assert "bronze.customerraw" in result.source_tables

def test_context_view_missing_profile() -> None:
    """run_context raises ValueError when view catalog has no profile."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        cat = json.loads(cat_path.read_text())
        del cat["profile"]
        cat_path.write_text(json.dumps(cat))
        with pytest.raises(ValueError, match="no 'profile' section"):
            refactor.run_context(root, "silver.vw_active_customers")

def test_context_view_missing_sql() -> None:
    """run_context raises ValueError when view catalog has no sql."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        cat = json.loads(cat_path.read_text())
        del cat["sql"]
        cat_path.write_text(json.dumps(cat))
        with pytest.raises(ValueError, match="no 'sql' key"):
            refactor.run_context(root, "silver.vw_active_customers")

def test_cli_context_view_success() -> None:
    """CLI context command returns view-specific JSON when given a view FQN."""
    result = _cli_runner.invoke(
        refactor.app,
        ["context", "--table", "silver.vw_active_customers", "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["object_type"] == "view"
    assert "view_sql" in data
    assert "writer" not in data
