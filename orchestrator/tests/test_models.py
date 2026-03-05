"""Tests for CandidateWriters Pydantic models against the contract schema."""

import pytest
from pydantic import ValidationError

from scoping_agent.models import (
    CandidateWriter,
    CandidateWritersOutput,
    ScopingResult,
    Summary,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# CandidateWriter
# ---------------------------------------------------------------------------


def test_candidate_writer_valid():
    w = CandidateWriter(
        procedure_name="dbo.usp_load_fact_sales",
        write_type="direct",
        call_path=["dbo.usp_load_fact_sales"],
        rationale="Direct INSERT found.",
        confidence=0.90,
    )
    assert w.confidence == 0.90
    assert w.write_type == "direct"


def test_candidate_writer_confidence_bounds():
    with pytest.raises(ValidationError):
        CandidateWriter(
            procedure_name="p",
            write_type="direct",
            call_path=["p"],
            rationale="r",
            confidence=1.1,
        )
    with pytest.raises(ValidationError):
        CandidateWriter(
            procedure_name="p",
            write_type="direct",
            call_path=["p"],
            rationale="r",
            confidence=-0.1,
        )


def test_candidate_writer_invalid_write_type():
    with pytest.raises(ValidationError):
        CandidateWriter(
            procedure_name="p",
            write_type="unknown",  # type: ignore[arg-type]
            call_path=["p"],
            rationale="r",
            confidence=0.5,
        )


# ---------------------------------------------------------------------------
# ScopingResult
# ---------------------------------------------------------------------------


def test_scoping_result_resolved():
    r = ScopingResult(
        item_id="dbo.fact_sales",
        status="resolved",
        selected_writer="dbo.usp_load_fact_sales",
        candidate_writers=[
            CandidateWriter(
                procedure_name="dbo.usp_load_fact_sales",
                write_type="direct",
                call_path=["dbo.usp_load_fact_sales"],
                rationale="Direct INSERT.",
                confidence=0.95,
            )
        ],
    )
    assert r.status == "resolved"
    assert r.selected_writer == "dbo.usp_load_fact_sales"


def test_scoping_result_no_writer_found():
    r = ScopingResult(item_id="dbo.dim_date", status="no_writer_found")
    assert r.candidate_writers == []
    assert r.selected_writer is None


def test_scoping_result_error():
    r = ScopingResult(
        item_id="dbo.cross_db_table",
        status="error",
        errors=["ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE"],
        validation=ValidationResult(
            passed=False,
            issues=["Cross-database reference detected"],
        ),
    )
    assert r.errors == ["ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE"]
    assert r.validation.passed is False


def test_scoping_result_invalid_status():
    with pytest.raises(ValidationError):
        ScopingResult(item_id="dbo.t", status="unknown_status")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# CandidateWritersOutput
# ---------------------------------------------------------------------------


def test_full_output_resolved():
    output = CandidateWritersOutput(
        batch_id="test-batch-id",
        results=[
            ScopingResult(
                item_id="dbo.fact_sales",
                status="resolved",
                selected_writer="dbo.usp_load_fact_sales",
                candidate_writers=[
                    CandidateWriter(
                        procedure_name="dbo.usp_load_fact_sales",
                        write_type="direct",
                        call_path=["dbo.usp_load_fact_sales"],
                        rationale="Direct INSERT.",
                        confidence=0.90,
                    )
                ],
            )
        ],
        summary=Summary(total=1, resolved=1),
    )
    assert output.schema_version == "1.0"
    assert output.summary.resolved == 1


def test_full_output_roundtrip():
    """JSON serialise then deserialise preserves all fields."""
    output = CandidateWritersOutput(
        batch_id="abc",
        results=[ScopingResult(item_id="dbo.t", status="no_writer_found")],
        summary=Summary(total=1, no_writer_found=1),
    )
    json_str = output.model_dump_json(exclude_none=True)
    restored = CandidateWritersOutput.model_validate_json(json_str)
    assert restored.batch_id == "abc"
    assert restored.summary.no_writer_found == 1
