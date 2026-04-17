"""Tests for shared.diagnostics.sqlserver — SQL Server dialect-specific checks."""

from __future__ import annotations

from pathlib import Path


from shared.diagnostics.sqlserver import (
    check_ambiguous_reference,
    check_cross_db_exec,
    check_dmf_error,
    check_dmf_misclassified,
    check_goto_detected,
    check_linked_server_exec,
    check_segmenter_limit,
    check_subtype_unknown,
)
from shared.loader_data import DdlEntry

from diagnostics_helpers import diag_empty_refs, diag_make_ctx


# ── AMBIGUOUS_REFERENCE ──────────────────────────────────────────────────────


class TestAmbiguousReference:

    def test_fires_when_ambiguous(self, tmp_path: Path) -> None:
        refs = diag_empty_refs()
        refs["tables"]["in_scope"].append({
            "schema": "dbo", "name": "Mystery",
            "is_selected": True, "is_updated": False, "is_ambiguous": True,
        })
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", {"references": refs})
        result = check_ambiguous_reference(ctx)
        assert result is not None
        assert len(result) == 1
        assert result[0].code == "AMBIGUOUS_REFERENCE"
        assert result[0].details["reference_fqn"] == "dbo.Mystery"
        assert result[0].details["reference_type"] == "tables"

    def test_none_when_not_ambiguous(self, tmp_path: Path) -> None:
        refs = diag_empty_refs()
        refs["tables"]["in_scope"].append({
            "schema": "dbo", "name": "Clear",
            "is_selected": True, "is_updated": False, "is_ambiguous": False,
        })
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", {"references": refs})
        result = check_ambiguous_reference(ctx)
        assert result is None


# ── DMF_MISCLASSIFIED ────────────────────────────────────────────────────────


class TestDmfMisclassified:

    def test_fires_when_table_is_actually_view(self, tmp_path: Path) -> None:
        refs = diag_empty_refs()
        refs["tables"]["in_scope"].append({
            "schema": "silver", "name": "vw_summary",
            "is_selected": True, "is_updated": False,
        })
        ctx = diag_make_ctx(
            tmp_path, "dbo.proc1", "procedure", {"references": refs},
            known_fqns={
                "tables": set(), "views": {"silver.vw_summary"},
                "functions": set(), "procedures": set(), "materialized_views": set(),
            },
        )
        result = check_dmf_misclassified(ctx)
        assert result is not None
        assert len(result) == 1
        assert result[0].details["likely_bucket"] == "views"

    def test_none_when_table_is_real_table(self, tmp_path: Path) -> None:
        refs = diag_empty_refs()
        refs["tables"]["in_scope"].append({
            "schema": "silver", "name": "FactSales",
            "is_selected": True, "is_updated": False,
        })
        ctx = diag_make_ctx(
            tmp_path, "dbo.proc1", "procedure", {"references": refs},
            known_fqns={
                "tables": {"silver.factsales"}, "views": set(),
                "functions": set(), "procedures": set(), "materialized_views": set(),
            },
        )
        result = check_dmf_misclassified(ctx)
        assert result is None


# ── DMF_ERROR ────────────────────────────────────────────────────────────────


class TestDmfError:

    def test_fires_when_dmf_errors_present(self, tmp_path: Path) -> None:
        catalog_data = {"references": diag_empty_refs(), "dmf_errors": ["ERROR: broken ref"]}
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_dmf_error(ctx)
        assert result is not None
        assert len(result) == 1
        assert result[0].code == "DMF_ERROR"
        assert result[0].severity == "error"
        assert result[0].details["error"] == "ERROR: broken ref"

    def test_none_when_no_dmf_errors(self, tmp_path: Path) -> None:
        catalog_data = {"references": diag_empty_refs()}
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_dmf_error(ctx)
        assert result is None


# ── SUBTYPE_UNKNOWN ──────────────────────────────────────────────────────────


class TestSubtypeUnknown:

    def test_fires_when_subtype_missing(self, tmp_path: Path) -> None:
        catalog_data = {"references": diag_empty_refs()}
        ctx = diag_make_ctx(tmp_path, "dbo.fn_calc", "function", catalog_data)
        result = check_subtype_unknown(ctx)
        assert result is not None
        assert result.code == "SUBTYPE_UNKNOWN"

    def test_none_when_subtype_present(self, tmp_path: Path) -> None:
        catalog_data = {"references": diag_empty_refs(), "subtype": "FN"}
        ctx = diag_make_ctx(tmp_path, "dbo.fn_calc", "function", catalog_data)
        result = check_subtype_unknown(ctx)
        assert result is None


# ── CROSS_DB_EXEC ────────────────────────────────────────────────────────────


class TestCrossDbExec:

    def test_fires_when_cross_db_exec(self, tmp_path: Path) -> None:
        catalog_data = {
            "references": diag_empty_refs(),
            "routing_reasons": ["cross_db_exec"],
        }
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_cross_db_exec(ctx)
        assert result is not None
        assert result.code == "CROSS_DB_EXEC"
        assert result.details["routing_reason"] == "cross_db_exec"

    def test_none_when_no_cross_db(self, tmp_path: Path) -> None:
        catalog_data = {
            "references": diag_empty_refs(),
            "routing_reasons": [],
        }
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_cross_db_exec(ctx)
        assert result is None


# ── LINKED_SERVER_EXEC ───────────────────────────────────────────────────────


class TestLinkedServerExec:

    def test_fires_when_linked_server_exec(self, tmp_path: Path) -> None:
        catalog_data = {
            "references": diag_empty_refs(),
            "routing_reasons": ["linked_server_exec"],
        }
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_linked_server_exec(ctx)
        assert result is not None
        assert result.code == "LINKED_SERVER_EXEC"

    def test_none_when_no_linked_server(self, tmp_path: Path) -> None:
        catalog_data = {
            "references": diag_empty_refs(),
            "routing_reasons": ["static_exec"],
        }
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_linked_server_exec(ctx)
        assert result is None


# ── GOTO_DETECTED ────────────────────────────────────────────────────────────


class TestGotoDetected:

    def test_fires_when_goto_in_ddl(self, tmp_path: Path) -> None:
        ddl = "CREATE PROCEDURE dbo.proc1 AS BEGIN GOTO retry; retry: PRINT 'x'; END"
        entry = DdlEntry(raw_ddl=ddl, ast=None)
        catalog_data = {"references": diag_empty_refs()}
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data, ddl_entry=entry)
        result = check_goto_detected(ctx)
        assert result is not None
        assert result.code == "GOTO_DETECTED"

    def test_none_when_no_goto(self, tmp_path: Path) -> None:
        ddl = "CREATE PROCEDURE dbo.proc1 AS BEGIN SELECT 1; END"
        entry = DdlEntry(raw_ddl=ddl, ast=None)
        catalog_data = {"references": diag_empty_refs()}
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data, ddl_entry=entry)
        result = check_goto_detected(ctx)
        assert result is None


# ── SEGMENTER_LIMIT ──────────────────────────────────────────────────────────


class TestSegmenterLimit:

    def test_fires_when_segmenter_error_present(self, tmp_path: Path) -> None:
        catalog_data = {
            "references": diag_empty_refs(),
            "segmenter_error": "maximum control-flow nesting depth exceeded",
        }
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_segmenter_limit(ctx)
        assert result is not None
        assert result.code == "SEGMENTER_LIMIT"
        assert "nesting depth" in result.message

    def test_none_when_no_segmenter_error(self, tmp_path: Path) -> None:
        catalog_data = {"references": diag_empty_refs()}
        ctx = diag_make_ctx(tmp_path, "dbo.proc1", "procedure", catalog_data)
        result = check_segmenter_limit(ctx)
        assert result is None
