"""Tests for scoping catalog model support module exports."""

from __future__ import annotations


def test_scoping_support_exports_scoping_models() -> None:
    from shared.catalog_model_support.scoping import (
        CandidateWriter,
        ScopingResultItem,
        ScopingSummary,
        ScopingSummaryCounts,
        SqlElement,
        TableScopingSection,
        ViewScopingSection,
    )

    section = TableScopingSection(
        status="resolved",
        candidates=[CandidateWriter(procedure_name="dbo.usp_load", rationale="Direct writer.")],
    )
    summary = ScopingSummary(
        schema_version="1.0",
        run_id="run-1",
        results=[ScopingResultItem(item_id="silver.t", status="resolved")],
        summary=ScopingSummaryCounts(
            total=1,
            resolved=1,
            ambiguous_multi_writer=0,
            no_writer_found=0,
            analyzed=0,
            error=0,
        ),
    )

    assert section.candidates[0].procedure_name == "dbo.usp_load"
    assert SqlElement(type="join", detail="JOIN x").type == "join"
    assert ViewScopingSection(status="analyzed").status == "analyzed"
    assert summary.results[0].status == "resolved"
