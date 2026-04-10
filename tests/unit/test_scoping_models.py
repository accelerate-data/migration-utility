"""Tests for scoping-related Pydantic models in catalog_models.py.

Validates construction, extra="forbid" rejection, and ScopingSummary contract.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.catalog_models import (
    CandidateWriter,
    DiagnosticsEntry,
    ScopingResultItem,
    ScopingSummary,
    ScopingSummaryCounts,
    SqlElement,
    TableScopingSection,
    ViewScopingSection,
)


# ── DiagnosticsEntry ──────────────────────────────────────────────────────


class TestDiagnosticsEntry:
    def test_valid_error(self) -> None:
        entry = DiagnosticsEntry(code="REMOTE_EXEC_UNSUPPORTED", message="Unsupported.", severity="error")
        assert entry.code == "REMOTE_EXEC_UNSUPPORTED"
        assert entry.severity == "error"

    def test_optional_fields(self) -> None:
        entry = DiagnosticsEntry(
            code="X", message="m", severity="warning",
            item_id="silver.t", field="scoping.status",
            details={"key": "value"},
        )
        assert entry.item_id == "silver.t"
        assert entry.details == {"key": "value"}

    def test_rejects_invalid_severity(self) -> None:
        with pytest.raises(ValidationError, match="severity"):
            DiagnosticsEntry(code="X", message="m", severity="info")

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            DiagnosticsEntry(code="X", message="m", severity="error", extra="nope")


# ── CandidateWriter ───────────────────────────────────────────────────────


class TestCandidateWriter:
    def test_valid_with_dependencies(self) -> None:
        cw = CandidateWriter(
            procedure_name="silver.usp_load",
            rationale="Primary writer.",
            dependencies={"tables": ["bronze.t"], "views": [], "functions": []},
        )
        assert cw.procedure_name == "silver.usp_load"
        assert cw.dependencies is not None

    def test_valid_without_dependencies(self) -> None:
        cw = CandidateWriter(procedure_name="silver.usp_delta", rationale="Delta writer.")
        assert cw.dependencies is None

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            CandidateWriter(procedure_name="p", rationale="r", write_type="direct")

    def test_rejects_missing_procedure_name(self) -> None:
        with pytest.raises(ValidationError, match="procedure_name"):
            CandidateWriter(rationale="r")


# ── TableScopingSection ──────────────────────────────────────────────────


class TestTableScopingSection:
    def test_valid_resolved(self) -> None:
        section = TableScopingSection(
            status="resolved",
            selected_writer="silver.usp_load",
            selected_writer_rationale="Only writer.",
            candidates=[CandidateWriter(procedure_name="silver.usp_load", rationale="Direct.")],
            warnings=[],
            errors=[],
        )
        assert section.status == "resolved"
        assert section.candidates is not None
        assert len(section.candidates) == 1

    def test_defaults(self) -> None:
        section = TableScopingSection()
        assert section.status == ""
        assert section.selected_writer is None
        assert section.candidates is None
        assert section.warnings == []
        assert section.errors == []

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            TableScopingSection(status="resolved", legacy_field="bad")

    def test_validates_from_dict(self) -> None:
        data = {
            "status": "resolved",
            "selected_writer": "silver.usp_load",
            "selected_writer_rationale": "Only writer.",
            "candidates": [{"procedure_name": "silver.usp_load", "rationale": "Direct."}],
            "warnings": [],
            "errors": [],
        }
        section = TableScopingSection.model_validate(data)
        assert section.candidates[0].procedure_name == "silver.usp_load"


# ── SqlElement ────────────────────────────────────────────────────────────


class TestSqlElement:
    def test_valid(self) -> None:
        elem = SqlElement(type="join", detail="INNER JOIN bronze.person")
        assert elem.type == "join"

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            SqlElement(type="join", detail="x", confidence=0.9)


# ── ViewScopingSection ───────────────────────────────────────────────────


class TestViewScopingSection:
    def test_valid_analyzed(self) -> None:
        section = ViewScopingSection(
            status="analyzed",
            sql_elements=[SqlElement(type="join", detail="JOIN t")],
            call_tree={"reads_from": ["t"], "views_referenced": []},
            logic_summary="Joins.",
            rationale="Simple.",
            warnings=[],
            errors=[],
        )
        assert section.status == "analyzed"

    def test_defaults(self) -> None:
        section = ViewScopingSection()
        assert section.sql_elements is None
        assert section.call_tree is None

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            ViewScopingSection(status="analyzed", unknown="bad")

    def test_validates_from_dict(self) -> None:
        data = {
            "status": "analyzed",
            "sql_elements": [{"type": "join", "detail": "INNER JOIN bronze.t"}],
            "call_tree": {"reads_from": ["bronze.t"], "views_referenced": []},
            "logic_summary": "Joins.",
            "rationale": "Simple.",
            "warnings": [],
            "errors": [],
        }
        section = ViewScopingSection.model_validate(data)
        assert section.sql_elements[0].type == "join"


# ── ScopingSummary ────────────────────────────────────────────────────────


class TestScopingSummary:
    def test_valid(self) -> None:
        summary = ScopingSummary(
            schema_version="1.0",
            run_id="550e8400-e29b-41d4-a716-446655440000",
            results=[
                ScopingResultItem(item_id="silver.dimcurrency", status="resolved"),
                ScopingResultItem(item_id="silver.dimdate", status="error"),
            ],
            summary=ScopingSummaryCounts(
                total=2, resolved=1, ambiguous_multi_writer=0, no_writer_found=0, error=1,
            ),
        )
        assert summary.schema_version == "1.0"
        assert len(summary.results) == 2

    def test_validates_from_dict(self) -> None:
        data = {
            "schema_version": "1.0",
            "run_id": "abc-123",
            "results": [
                {"item_id": "silver.t1", "status": "resolved"},
                {"item_id": "silver.t2", "status": "no_writer_found"},
            ],
            "summary": {
                "total": 2, "resolved": 1,
                "ambiguous_multi_writer": 0, "no_writer_found": 1, "error": 0,
            },
        }
        summary = ScopingSummary.model_validate(data)
        assert summary.results[1].status == "no_writer_found"

    def test_rejects_invalid_schema_version(self) -> None:
        with pytest.raises(ValidationError, match="schema_version"):
            ScopingSummary(
                schema_version="2.0",
                run_id="x",
                results=[],
                summary=ScopingSummaryCounts(
                    total=0, resolved=0, ambiguous_multi_writer=0, no_writer_found=0, error=0,
                ),
            )

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            ScopingSummary(
                schema_version="1.0",
                run_id="x",
                results=[],
                summary=ScopingSummaryCounts(
                    total=0, resolved=0, ambiguous_multi_writer=0, no_writer_found=0, error=0,
                ),
                extra_field="bad",
            )

    def test_rejects_missing_required(self) -> None:
        with pytest.raises(ValidationError):
            ScopingSummary(schema_version="1.0", run_id="x")

    def test_rejects_invalid_item_status(self) -> None:
        with pytest.raises(ValidationError, match="status"):
            ScopingResultItem(item_id="silver.t", status="unknown")

    def test_summary_counts_rejects_extra(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            ScopingSummaryCounts(
                total=1, resolved=1, ambiguous_multi_writer=0,
                no_writer_found=0, error=0, partial=0,
            )
