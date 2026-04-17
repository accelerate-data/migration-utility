from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared import dry_run
from shared import dry_run_core
from shared import generate_sources as gen_src
from shared.dry_run_support import status as dry_run_status
from shared.output_models.dry_run import DryRunOutput
from tests.unit.dry_run.dry_run_test_helpers import (
    _add_source_table,
    _add_table_to_project,
    _cli_runner,
    _make_bare_project,
    _make_exclude_project,
    _make_project,
    _make_reset_project,
)

def test_status_single_object() -> None:
    """Status for a single object returns all stage statuses."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.DimCustomer")
        assert result.fqn == "silver.dimcustomer"
        assert result.type == "table"
        assert result.stages.scope == "ok"
        assert result.stages.profile == "ok"
        assert result.stages.test_gen == "ok"

def test_status_all_objects() -> None:
    """Status with no FQN returns all objects with summary."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root)
        assert result.summary.total > 0
        # Check that silver.dimcustomer is in the list
        fqns = [obj.fqn for obj in result.objects]
        assert "silver.dimcustomer" in fqns

def test_status_all_objects_excludes_seed_tables() -> None:
    """Bulk status excludes seed tables from active migration stage counts."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root)

    fqns = [obj.fqn for obj in result.objects]
    assert "silver.dimcustomer" not in fqns

def test_status_source_table_detail_is_workflow_exempt() -> None:
    """Single-object status reports source tables as workflow-exempt."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root, "silver.DimCustomer")

    assert result.fqn == "silver.dimcustomer"
    assert result.type == "table"
    assert result.stages.scope == "N/A"
    assert result.stages.profile == "N/A"
    assert result.stages.test_gen == "N/A"
    assert result.stages.refactor == "N/A"
    assert result.stages.generate == "N/A"

def test_status_seed_table_detail_is_workflow_exempt() -> None:
    """Single-object status reports seed tables as workflow-exempt."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        result = dry_run.run_status(root, "silver.DimCustomer")

    assert result.fqn == "silver.dimcustomer"
    assert result.type == "table"
    assert result.stages.scope == "N/A"
    assert result.stages.profile == "N/A"
    assert result.stages.test_gen == "N/A"
    assert result.stages.refactor == "N/A"
    assert result.stages.generate == "N/A"

def test_status_all_objects_skips_summary_count_for_missing_status_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bulk status should not crash if a single-object probe returns StatusOutput."""
    tmp, root = _make_project()
    original = dry_run_status._single_object_status

    def _fake_single_object_status(*args, **kwargs):
        norm_fqn = args[1]
        if norm_fqn == "silver.dimcustomer":
            return dry_run_core.StatusOutput(fqn=norm_fqn, type=None, stages=None)
        return original(*args, **kwargs)

    with tmp:
        monkeypatch.setattr(dry_run_status, "_single_object_status", _fake_single_object_status)
        result = dry_run.run_status(root)

    assert result.summary is not None
    assert result.summary.total > 0

def test_status_view_object() -> None:
    """Status for a view returns correct type and stages."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["scoping"] = {"status": "analyzed", "sql_elements": [], "logic_summary": "test"}
        view_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_status(root, "silver.vDimSalesTerritory")
        assert result.type == "view"
        assert result.stages.scope == "ok"

def test_status_view_logs_warning_on_corrupt_catalog(caplog: pytest.LogCaptureFixture) -> None:
    """A corrupt view catalog JSON logs a warning and degrades gracefully."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        view_path.write_text("{bad json", encoding="utf-8")
        with caplog.at_level("WARNING"):
            result = dry_run.run_status(root, "silver.vDimSalesTerritory")
        assert result.type == "view"
        assert result.stages.scope is None
        assert any("view_catalog_load_failed" in r.message for r in caplog.records)

def test_status_pending_scope_preserves_specific_status() -> None:
    """Status output preserves incomplete scope states verbatim."""
    tmp, root = _make_project()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(table_path.read_text(encoding="utf-8"))
        cat["scoping"]["status"] = "ambiguous_multi_writer"
        table_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_status(root, "silver.DimCustomer")
        assert result.stages.scope == "ambiguous_multi_writer"

def test_status_mv_object() -> None:
    """Status for a materialized view returns type=mv."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.mv_FactSales")
        assert result.type == "mv"

def test_status_single_missing_object_reports_not_found() -> None:
    """Single-object status should not fabricate a table for missing objects."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_status(root, "silver.Missing")
        assert result.fqn == "silver.missing"
        assert result.type is None
        assert result.stages is None

def test_cli_status_single() -> None:
    """CLI status with FQN returns single-object status."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["status", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["fqn"] == "silver.dimcustomer"

def test_cli_status_all() -> None:
    """CLI status without FQN returns all objects."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["status", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert "objects" in output
        assert "summary" in output
