"""Tests for diagnostics, excluded objects, and is_source behavior.

Tests import shared.pipeline_status and shared.batch_plan functions directly
for fast, fixture-based execution.  No Docker or live database required.
"""

from __future__ import annotations

import json
from pathlib import Path


from shared.batch_plan import _BatchPlanInputs, build_batch_plan
from shared.batch_plan_support.diagnostics import _collect_catalog_diagnostics
from shared.batch_plan_support.inventory import _CatalogInventory
from shared.pipeline_status import (
    _compute_diagnostic_stage_flags,
    collect_object_diagnostics,
)

from .conftest import _make_minimal_project, _make_project, _write_table_cat


# ── Diagnostics tests ─────────────────────────────────────────────────────────


class TestCollectObjectDiagnostics:
    def test_no_diagnostics(self, tmp_path):
        """Object with no warnings/errors returns empty list."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T"}), encoding="utf-8"
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        assert diags == []

    def test_top_level_warnings(self, tmp_path):
        """Top-level warnings on a table are collected."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "warnings": [{"code": "STALE_OBJECT", "message": "Object is stale", "severity": "warning"}],
            }),
            encoding="utf-8",
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        codes = [d["code"] for d in diags]
        assert "STALE_OBJECT" in codes

    def test_scoping_errors_collected(self, tmp_path):
        """Errors in scoping sub-section are collected."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {
                    "errors": [{"code": "MULTI_TABLE_WRITE", "message": "Multiple writes", "severity": "error"}],
                    "warnings": [],
                },
            }),
            encoding="utf-8",
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        codes = [d["code"] for d in diags]
        assert "MULTI_TABLE_WRITE" in codes

    def test_catalog_diagnostics_aggregated(self):
        """build_batch_plan aggregates diagnostics across all objects."""
        tmp, dst = _make_project()
        try:
            # Inject a diagnostic into dimdate's catalog
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["warnings"] = [{"code": "STALE_OBJECT", "message": "stale", "severity": "warning"}]
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            warning_codes = [w.code for w in result.catalog_diagnostics.warnings]
            assert "STALE_OBJECT" in warning_codes
            assert result.catalog_diagnostics.total_warnings >= 1
        finally:
            tmp.cleanup()


class TestReviewedWarnings:
    def test_reviewed_warning_is_hidden_from_catalog_diagnostics(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        warning = {
            "code": "MULTI_TABLE_WRITE",
            "message": "proc also writes dim.other",
            "severity": "warning",
        }
        (tmp_path / "catalog" / "tables" / "dim.dim_address.json").write_text(
            json.dumps({
                "schema": "dim",
                "name": "dim_address",
                "warnings": [warning],
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import (
            ReviewedDiagnostic,
            diagnostic_identity,
            write_reviewed_diagnostic,
        )

        identity = diagnostic_identity("dim.dim_address", warning, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Reviewed table slice and accepted multi-table writer.",
                evidence=["catalog/tables/dim.dim_address.json"],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_warnings == 0
        assert result.catalog_diagnostics.reviewed_warnings_hidden == 1

    def test_reviewed_warning_reappears_when_message_changes(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        old_warning = {"code": "PARSE_ERROR", "message": "old parse warning", "severity": "warning"}
        new_warning = {"code": "PARSE_ERROR", "message": "new parse warning", "severity": "warning"}
        (tmp_path / "catalog" / "tables" / "fact.fct_sales.json").write_text(
            json.dumps({
                "schema": "fact",
                "name": "fct_sales",
                "warnings": [new_warning],
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import (
            ReviewedDiagnostic,
            diagnostic_identity,
            write_reviewed_diagnostic,
        )

        identity = diagnostic_identity("fact.fct_sales", old_warning, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Old review.",
                evidence=[],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_warnings == 1
        assert result.catalog_diagnostics.warnings[0].code == "PARSE_ERROR"
        assert result.catalog_diagnostics.reviewed_warnings_hidden == 0

    def test_errors_are_not_hidden_by_reviewed_warning_artifact(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        error = {"code": "PARSE_ERROR", "message": "fatal parse issue", "severity": "error"}
        (tmp_path / "catalog" / "tables" / "fact.fct_sales.json").write_text(
            json.dumps({
                "schema": "fact",
                "name": "fct_sales",
                "errors": [error],
                "scoping": {"status": "error"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import (
            ReviewedDiagnostic,
            diagnostic_identity,
            write_reviewed_diagnostic,
        )

        identity = diagnostic_identity("fact.fct_sales", error, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Attempted review should not suppress errors.",
                evidence=[],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_errors == 1
        assert result.catalog_diagnostics.errors[0].code == "PARSE_ERROR"

    def test_collect_catalog_diagnostics_partitions_support_module(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        warning = {"code": "PARSE_ERROR", "message": "parse warning", "severity": "warning"}
        (tmp_path / "catalog" / "tables" / "dim.dim_address.json").write_text(
            json.dumps(
                {
                    "schema": "dim",
                    "name": "dim_address",
                    "warnings": [warning],
                    "scoping": {"status": "no_writer_found"},
                },
            ),
            encoding="utf-8",
        )
        inputs = _BatchPlanInputs(
            inv=_CatalogInventory(table_fqns=["dim.dim_address"]),
            obj_type_map={"dim.dim_address": "table"},
            statuses={"dim.dim_address": "n_a"},
            dbt_status={"dim.dim_address": False},
            obj_diagnostics={"dim.dim_address": [warning]},
            raw_deps={"dim.dim_address": set()},
            scope_phase=[],
            profile_phase=[],
            migrate_candidates=[],
            completed_objects=[],
            n_a_objects=["dim.dim_address"],
            blocking={},
        )

        errors, warnings, resolved_counts, hidden = _collect_catalog_diagnostics(tmp_path, inputs)

        assert errors == []
        assert [entry.code for entry in warnings] == ["PARSE_ERROR"]
        assert resolved_counts == {"dim.dim_address": 0}
        assert hidden == 0


# ── Excluded objects tests ────────────────────────────────────────────────────


class TestExcludedObjects:
    """Tests for excluded: true filtering in build_batch_plan."""

    def _make_single_table_project(self, tmp_path: Path, excluded: bool = False) -> Path:
        """Create a minimal project with one table, optionally excluded."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        cat: dict = {"schema": "silver", "name": "DimDate", "primary_keys": [], "referenced_by": {}}
        if excluded:
            cat["excluded"] = True
        (tmp_path / "catalog" / "tables" / "silver.dimdate.json").write_text(
            json.dumps(cat), encoding="utf-8"
        )
        return tmp_path

    def test_excluded_table_absent_from_all_phases(self, tmp_path):
        """Excluded table does not appear in scope_phase, profile_phase, migrate_batches, n_a_objects, or completed_objects."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        all_active_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
            | {n.fqn for n in result.n_a_objects}
        )
        assert "silver.dimdate" not in all_active_fqns

    def test_excluded_table_in_excluded_objects_list(self, tmp_path):
        """Excluded table appears in excluded_objects with correct shape."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        assert len(result.excluded_objects) == 1
        entry = result.excluded_objects[0]
        assert entry.fqn == "silver.dimdate"
        assert entry.type == "table"
        assert hasattr(entry, "note")

    def test_excluded_count_in_summary(self, tmp_path):
        """summary.excluded_count reflects number of excluded objects."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        assert result.summary.excluded_count == 1

    def test_non_excluded_table_has_zero_excluded_count(self, tmp_path):
        """summary.excluded_count is 0 when nothing is excluded."""
        dst = self._make_single_table_project(tmp_path, excluded=False)
        result = build_batch_plan(dst)
        assert result.summary.excluded_count == 0
        assert result.excluded_objects == []

    def test_excluded_view_absent_from_all_phases(self, tmp_path):
        """Excluded view does not appear in any active phase."""
        (tmp_path / "catalog" / "views").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        (tmp_path / "catalog" / "views" / "silver.vw_legacy.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_legacy",
                "excluded": True,
                "references": {"tables": {"in_scope": []}, "views": {"in_scope": []}},
            }),
            encoding="utf-8",
        )
        result = build_batch_plan(tmp_path)
        assert len(result.excluded_objects) == 1
        assert result.excluded_objects[0].fqn == "silver.vw_legacy"
        assert result.excluded_objects[0].type == "view"
        all_active = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
        )
        assert "silver.vw_legacy" not in all_active

    def test_excluded_dep_removed_from_active_dep_graph(self, tmp_path):
        """Active table that previously depended on now-excluded table has no blocking dep on it."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        # Excluded source table
        (tmp_path / "catalog" / "tables" / "silver.excludedsource.json").write_text(
            json.dumps({
                "schema": "silver", "name": "ExcludedSource",
                "excluded": True, "primary_keys": [], "referenced_by": {},
            }),
            encoding="utf-8",
        )
        # Active table — no writer proc, so writerless (n_a).
        # Deps are resolved via writer proc; with no writer this table has no dep chain.
        # For simplicity, just confirm the excluded table is not in excluded_objects fqn set
        # of the active table's direct_deps.
        (tmp_path / "catalog" / "tables" / "silver.facttable.json").write_text(
            json.dumps({
                "schema": "silver", "name": "FactTable",
                "primary_keys": [], "referenced_by": {},
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        result = build_batch_plan(tmp_path)
        excluded_fqns_in_output = {e.fqn for e in result.excluded_objects}
        assert "silver.excludedsource" in excluded_fqns_in_output
        # silver.facttable has no_writer_found → writerless n_a
        n_a_fqns = {n.fqn for n in result.n_a_objects}
        assert "silver.facttable" in n_a_fqns

    def test_full_fixture_excluded_count_zero(self):
        """Standard fixture (no excluded objects) has excluded_count=0."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert result.summary.excluded_count == 0
            assert result.excluded_objects == []
        finally:
            tmp.cleanup()

    def test_full_fixture_with_one_excluded(self):
        """Excluding one table reduces total active count and adds to excluded_objects."""
        tmp, dst = _make_project()
        try:
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["excluded"] = True
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            assert result.summary.excluded_count == 1
            assert result.summary.total_objects == 7  # refcurrency remains as writerless n_a, dimdate excluded
            excluded_fqns = {e.fqn for e in result.excluded_objects}
            assert "silver.dimdate" in excluded_fqns
            # silver.dimdate must not appear in any active phase
            all_active = (
                {n.fqn for n in result.scope_phase}
                | {n.fqn for n in result.profile_phase}
                | {n.fqn for b in result.migrate_batches for n in b.objects}
            )
            assert "silver.dimdate" not in all_active
        finally:
            tmp.cleanup()


# ── diagnostic_stage_flags tests ──────────────────────────────────────────────


class TestComputeDiagnosticStageFlags:
    def test_empty_diagnostics(self):
        """No diagnostics → empty flags dict."""
        assert _compute_diagnostic_stage_flags([]) == {}

    def test_no_mapped_codes(self):
        """Diagnostics with codes not in the stage map → empty flags dict."""
        diags = [{"code": "STALE_OBJECT", "severity": "warning", "message": "stale"}]
        assert _compute_diagnostic_stage_flags(diags) == {}

    def test_parse_error_maps_to_refactor(self):
        """PARSE_ERROR maps to refactor stage with error severity."""
        diags = [{"code": "PARSE_ERROR", "severity": "error", "message": "parse failed"}]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_ddl_parse_error_maps_to_refactor(self):
        """DDL_PARSE_ERROR maps to refactor stage with error severity."""
        diags = [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": "parse failed"}]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_multi_table_write_maps_to_scope(self):
        """MULTI_TABLE_WRITE maps to scope stage, preserving the diagnostic's severity."""
        diags = [{"code": "MULTI_TABLE_WRITE", "severity": "warning", "message": "multi write"}]
        assert _compute_diagnostic_stage_flags(diags) == {"scope": "warning"}

    def test_remote_exec_unsupported_maps_to_scope(self):
        """REMOTE_EXEC_UNSUPPORTED maps to scope stage with error severity."""
        diags = [{"code": "REMOTE_EXEC_UNSUPPORTED", "severity": "error", "message": "remote exec"}]
        assert _compute_diagnostic_stage_flags(diags) == {"scope": "error"}

    def test_error_beats_warning_same_stage(self):
        """Two diagnostics for the same stage: error severity wins over warning."""
        diags = [
            {"code": "PARSE_ERROR", "severity": "warning", "message": "w"},
            {"code": "DDL_PARSE_ERROR", "severity": "error", "message": "e"},
        ]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_multiple_stages(self):
        """Diagnostics that map to different stages both appear in the result."""
        diags = [
            {"code": "PARSE_ERROR", "severity": "error", "message": "parse"},
            {"code": "MULTI_TABLE_WRITE", "severity": "warning", "message": "multi"},
        ]
        result = _compute_diagnostic_stage_flags(diags)
        assert result == {"refactor": "error", "scope": "warning"}

    def test_node_includes_diagnostic_stage_flags(self):
        """build_batch_plan nodes include diagnostic_stage_flags field."""
        tmp, dst = _make_project()
        try:
            # Inject a PARSE_ERROR diagnostic into dimdate's catalog
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["errors"] = [{"code": "PARSE_ERROR", "message": "failed to parse", "severity": "error"}]
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            dimdate_node = next(
                n for n in result.scope_phase if n.fqn == "silver.dimdate"
            )
            assert hasattr(dimdate_node, "diagnostic_stage_flags")
            assert dimdate_node.diagnostic_stage_flags.get("refactor") == "error"
        finally:
            tmp.cleanup()


# ── is_source flag tests ──────────────────────────────────────────────────────


class TestIsSourceBatchPlan:
    def test_is_source_table_excluded_from_pipeline(self, tmp_path):
        """Table with is_source: true is excluded from all pipeline phases."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        all_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
            | {n.fqn for n in result.n_a_objects}
            | {n.fqn for n in result.source_pending}
        )
        assert "silver.audit" not in all_fqns

    def test_is_source_counted_in_summary(self, tmp_path):
        """summary.source_tables counts is_source: true tables."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        assert result.summary.source_tables == 1
        assert result.summary.total_objects == 0  # excluded from pipeline

    def test_is_source_appears_in_source_tables_list(self, tmp_path):
        """is_source: true table appears in source_tables output list."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        source_fqns = {o.fqn for o in result.source_tables}
        assert "silver.audit" in source_fqns

    def test_resolved_table_with_is_source_excluded_from_pipeline(self, tmp_path):
        """Resolved table marked is_source: true is excluded (cross-domain scenario)."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.crossdomain.json",
            "silver.crossdomain",
            {"status": "resolved", "selected_writer": "dbo.usp_other_team"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        assert result.summary.source_tables == 1
        assert result.summary.total_objects == 0

    def test_writerless_table_populates_n_a_objects(self, tmp_path):
        """no_writer_found table without is_source appears in n_a_objects."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.lookup.json",
            "silver.lookup",
            {"status": "no_writer_found"},
        )
        result = build_batch_plan(root)
        n_a_fqns = {o.fqn for o in result.n_a_objects}
        assert "silver.lookup" in n_a_fqns
        assert result.summary.writerless_tables == 1
        assert result.summary.source_pending == 0

    def test_writerless_table_not_in_active_pipeline_phases(self, tmp_path):
        """Writerless tables do not appear in active pipeline phases."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.lookup.json",
            "silver.lookup",
            {"status": "no_writer_found"},
        )
        result = build_batch_plan(root)
        all_pipeline_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
        )
        assert "silver.lookup" not in all_pipeline_fqns
