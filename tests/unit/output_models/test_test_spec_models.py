"""Tests for test-spec and test-review Pydantic models in output_models.py.

Validates construction, extra="forbid" rejection, defaults, and nested types.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.output_models.test_specs import (
    BranchEntry,
    CommandSummary,
    CoverageSection,
    ExpectEntry,
    GeneratorFeedback,
    GivenEntry,
    QualityIssue,
    ReviewerBranchEntry,
    TestReviewOutput,
    TestSpec,
    TestSpecOutput,
    UnitTestEntry,
    ValidationSection,
)


# ── BranchEntry ──────────────────────────────────────────────────────────


class TestBranchEntry:
    def test_valid(self) -> None:
        b = BranchEntry(id="merge_matched", statement_index=0, description="MERGE MATCHED", scenarios=["test_a"])
        assert b.id == "merge_matched"
        assert b.statement_index == 0
        assert b.scenarios == ["test_a"]

    def test_empty_scenarios(self) -> None:
        b = BranchEntry(id="x", statement_index=1, description="d", scenarios=[])
        assert b.scenarios == []

    def test_negative_statement_index_rejected(self) -> None:
        with pytest.raises(ValidationError, match="statement_index"):
            BranchEntry(id="x", statement_index=-1, description="d", scenarios=[])

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            BranchEntry(id="x", statement_index=0, description="d", scenarios=[], extra="nope")


# ── GivenEntry ───────────────────────────────────────────────────────────


class TestGivenEntry:
    def test_valid(self) -> None:
        g = GivenEntry(table="[bronze].[Product]", rows=[{"id": 1}])
        assert g.table == "[bronze].[Product]"
        assert g.rows == [{"id": 1}]

    def test_empty_rows_allowed(self) -> None:
        g = GivenEntry(table="[dbo].[T]", rows=[])
        assert g.rows == []

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            GivenEntry(table="t", rows=[], extra="nope")


# ── ExpectEntry ──────────────────────────────────────────────────────────


class TestExpectEntry:
    def test_valid(self) -> None:
        e = ExpectEntry(rows=[{"product_key": 1}])
        assert e.rows == [{"product_key": 1}]

    def test_empty_rows(self) -> None:
        e = ExpectEntry(rows=[])
        assert e.rows == []


# ── UnitTestEntry ────────────────────────────────────────────────────────


class TestUnitTestEntry:
    def test_procedure_based(self) -> None:
        u = UnitTestEntry(
            name="test_merge_matched",
            target_table="[silver].[DimProduct]",
            procedure="[silver].[usp_load_DimProduct]",
            given=[GivenEntry(table="[bronze].[Product]", rows=[{"id": 1}])],
            expect=ExpectEntry(rows=[{"key": 1}]),
        )
        assert u.name == "test_merge_matched"
        assert u.procedure == "[silver].[usp_load_DimProduct]"
        assert u.sql is None

    def test_view_based(self) -> None:
        u = UnitTestEntry(
            name="test_view_simple",
            sql="SELECT * FROM [silver].[vw_Sales]",
            given=[GivenEntry(table="[bronze].[Sales]", rows=[{"id": 1}])],
        )
        assert u.sql is not None
        assert u.procedure is None
        assert u.target_table is None
        assert u.expect is None

    def test_given_requires_at_least_one(self) -> None:
        with pytest.raises(ValidationError, match="given"):
            UnitTestEntry(name="test_empty", given=[])

    def test_optional_model_field(self) -> None:
        u = UnitTestEntry(
            name="test_x",
            model="dimproduct",
            given=[GivenEntry(table="[t]", rows=[{"a": 1}])],
        )
        assert u.model == "dimproduct"

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            UnitTestEntry(
                name="test_x",
                given=[GivenEntry(table="[t]", rows=[{"a": 1}])],
                extra="nope",
            )


# ── ValidationSection ────────────────────────────────────────────────────


class TestValidationSection:
    def test_passed(self) -> None:
        v = ValidationSection(passed=True, issues=[])
        assert v.passed is True
        assert v.issues == []

    def test_with_issues(self) -> None:
        v = ValidationSection(
            passed=False,
            issues=[{"code": "MISSING_COLUMN", "message": "col missing", "severity": "error"}],
        )
        assert v.passed is False
        assert len(v.issues) == 1


# ── TestSpec ─────────────────────────────────────────────────────────────


def _minimal_spec_data(**overrides: object) -> dict:
    """Build a minimal valid TestSpec dict, with optional overrides."""
    base = {
        "item_id": "silver.dimproduct",
        "status": "ok",
        "coverage": "complete",
        "branch_manifest": [
            {"id": "merge_matched", "statement_index": 0, "description": "MERGE MATCHED", "scenarios": ["test_a"]},
        ],
        "unit_tests": [
            {
                "name": "test_a",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load_DimProduct]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
        ],
        "uncovered_branches": [],
        "warnings": [],
        "validation": {"passed": True, "issues": []},
        "errors": [],
    }
    base.update(overrides)
    return base


class TestTestSpec:
    def test_minimal_valid(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data())
        assert spec.item_id == "silver.dimproduct"
        assert spec.object_type == "table"
        assert spec.status == "ok"

    def test_object_type_defaults_to_table(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data())
        assert spec.object_type == "table"

    def test_object_type_view(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data(object_type="view"))
        assert spec.object_type == "view"

    def test_object_type_mv(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data(object_type="mv"))
        assert spec.object_type == "mv"

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError, match="status"):
            TestSpec.model_validate(_minimal_spec_data(status="unknown"))

    def test_invalid_coverage_rejected(self) -> None:
        with pytest.raises(ValidationError, match="coverage"):
            TestSpec.model_validate(_minimal_spec_data(coverage="full"))

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            TestSpec.model_validate(_minimal_spec_data(extra="nope"))

    def test_with_warnings_and_errors(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data(
            warnings=[{"code": "W1", "message": "warn", "severity": "warning"}],
            errors=[{"code": "E1", "message": "err", "severity": "error"}],
        ))
        assert len(spec.warnings) == 1
        assert len(spec.errors) == 1

    def test_uncovered_branches(self) -> None:
        spec = TestSpec.model_validate(_minimal_spec_data(
            uncovered_branches=["branch_x", "branch_y"],
            coverage="partial",
        ))
        assert spec.uncovered_branches == ["branch_x", "branch_y"]


# ── TestSpecOutput ───────────────────────────────────────────────────────


class TestTestSpecOutput:
    def test_valid(self) -> None:
        output = TestSpecOutput.model_validate({
            "schema_version": "1.0",
            "results": [_minimal_spec_data()],
            "summary": {"total": 1, "ok": 1, "partial": 0, "error": 0},
        })
        assert isinstance(output, TestSpecOutput)
        assert output.schema_version == "1.0"
        assert len(output.results) == 1
        assert output.summary.total == 1

    def test_wrong_schema_version_rejected(self) -> None:
        with pytest.raises(ValidationError, match="schema_version"):
            TestSpecOutput.model_validate({
                "schema_version": "2.0",
                "results": [],
                "summary": {"total": 0, "ok": 0, "partial": 0, "error": 0},
            })

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            TestSpecOutput.model_validate({
                "schema_version": "1.0",
                "results": [],
                "summary": {"total": 0, "ok": 0, "partial": 0, "error": 0},
                "extra": "nope",
            })


# ── CommandSummary ───────────────────────────────────────────────────────


class TestCommandSummary:
    def test_valid(self) -> None:
        s = CommandSummary(total=3, ok=2, partial=1, error=0)
        assert s.total == 3

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError, match="total"):
            CommandSummary(total=-1, ok=0, partial=0, error=0)


# ── ReviewerBranchEntry ─────────────────────────────────────────────────


class TestReviewerBranchEntry:
    def test_valid(self) -> None:
        r = ReviewerBranchEntry(id="merge_matched", description="MERGE", covered=True, covering_scenarios=["test_a"])
        assert r.covered is True
        assert r.covering_scenarios == ["test_a"]

    def test_uncovered(self) -> None:
        r = ReviewerBranchEntry(id="x", description="d", covered=False, covering_scenarios=[])
        assert r.covered is False

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            ReviewerBranchEntry(id="x", description="d", covered=True, covering_scenarios=[], extra="nope")


# ── CoverageSection ─────────────────────────────────────────────────────


class TestCoverageSection:
    def test_complete(self) -> None:
        c = CoverageSection(total_branches=5, covered_branches=5, score="complete")
        assert c.untestable_branches == 0
        assert c.uncovered == []
        assert c.untestable == []

    def test_partial_with_uncovered(self) -> None:
        c = CoverageSection(
            total_branches=3,
            covered_branches=1,
            score="partial",
            uncovered=[{"id": "branch_x", "description": "missing"}],
        )
        assert len(c.uncovered) == 1
        assert c.uncovered[0].id == "branch_x"

    def test_with_untestable(self) -> None:
        c = CoverageSection(
            total_branches=3,
            covered_branches=2,
            untestable_branches=1,
            score="complete",
            untestable=[{"id": "dynamic_sql", "description": "dynamic", "rationale": "needs runtime"}],
        )
        assert len(c.untestable) == 1
        assert c.untestable[0].rationale == "needs runtime"

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError, match="total_branches"):
            CoverageSection(total_branches=-1, covered_branches=0, score="partial")


# ── QualityIssue ─────────────────────────────────────────────────────────


class TestQualityIssue:
    def test_valid(self) -> None:
        q = QualityIssue(scenario="test_a", issue="Missing NOT NULL column", severity="error")
        assert q.severity == "error"

    def test_invalid_severity_rejected(self) -> None:
        with pytest.raises(ValidationError, match="severity"):
            QualityIssue(scenario="test_a", issue="x", severity="info")


# ── GeneratorFeedback ────────────────────────────────────────────────────


class TestGeneratorFeedback:
    def test_valid(self) -> None:
        f = GeneratorFeedback(uncovered_branches=["branch_x"], quality_fixes=["Fix test_a"])
        assert f.uncovered_branches == ["branch_x"]

    def test_empty(self) -> None:
        f = GeneratorFeedback(uncovered_branches=[], quality_fixes=[])
        assert f.uncovered_branches == []


# ── TestReviewOutput ─────────────────────────────────────────────────────


def _minimal_review_data(**overrides: object) -> dict:
    """Build a minimal valid TestReviewOutput dict."""
    base = {
        "item_id": "silver.dimproduct",
        "status": "approved",
        "reviewer_branch_manifest": [
            {"id": "merge_matched", "description": "MERGE MATCHED", "covered": True, "covering_scenarios": ["test_a"]},
        ],
        "coverage": {
            "total_branches": 1,
            "covered_branches": 1,
            "score": "complete",
            "uncovered": [],
        },
        "quality_issues": [],
        "feedback_for_generator": {
            "uncovered_branches": [],
            "quality_fixes": [],
        },
        "warnings": [],
        "errors": [],
    }
    base.update(overrides)
    return base


class TestTestReviewOutput:
    def test_approved(self) -> None:
        review = TestReviewOutput.model_validate(_minimal_review_data())
        assert review.status == "approved"
        assert review.coverage.score == "complete"

    def test_revision_requested(self) -> None:
        review = TestReviewOutput.model_validate(_minimal_review_data(
            status="revision_requested",
            coverage={
                "total_branches": 3,
                "covered_branches": 1,
                "score": "partial",
                "uncovered": [{"id": "b1", "description": "missing branch"}],
            },
            feedback_for_generator={
                "uncovered_branches": ["b1"],
                "quality_fixes": ["Add scenario for b1"],
            },
        ))
        assert review.status == "revision_requested"
        assert len(review.coverage.uncovered) == 1
        assert review.feedback_for_generator.uncovered_branches == ["b1"]

    def test_approved_with_warnings(self) -> None:
        review = TestReviewOutput.model_validate(_minimal_review_data(status="approved_with_warnings"))
        assert review.status == "approved_with_warnings"

    def test_error_status(self) -> None:
        review = TestReviewOutput.model_validate(_minimal_review_data(
            status="error",
            errors=[{"code": "CONTEXT_PREREQUISITE_MISSING", "message": "missing", "severity": "error"}],
        ))
        assert review.status == "error"
        assert len(review.errors) == 1

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError, match="status"):
            TestReviewOutput.model_validate(_minimal_review_data(status="unknown"))

    def test_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            TestReviewOutput.model_validate(_minimal_review_data(extra="nope"))

    def test_with_quality_issues(self) -> None:
        review = TestReviewOutput.model_validate(_minimal_review_data(
            quality_issues=[{"scenario": "test_a", "issue": "Missing FK", "severity": "warning"}],
        ))
        assert len(review.quality_issues) == 1
        assert review.quality_issues[0].severity == "warning"
