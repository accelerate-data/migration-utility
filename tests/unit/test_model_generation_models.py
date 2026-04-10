"""Unit tests for model generation / review Pydantic contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.output_models import (
    ArtifactPaths,
    CheckResult,
    ExecutionInfo,
    FeedbackItem,
    GeneratedInfo,
    GeneratedModelInfo,
    GeneratedYamlInfo,
    GeneratorItem,
    ModelGenerationHandoff,
    ModelGenerationItemOutput,
    ModelGenerationOutput,
    ModelGeneratorInput,
    ModelReviewOutput,
    RenderUnitTestsOutput,
    ReviewChecks,
    ReviewInfo,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


def _generator_item(**overrides: object) -> dict:
    base = {"item_id": "silver.dimcustomer", "selected_writer": "silver.usp_load_dimcustomer"}
    return {**base, **overrides}


def _generator_input(**overrides: object) -> dict:
    base = {
        "schema_version": "2.0",
        "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "items": [_generator_item()],
    }
    return {**base, **overrides}


def _artifact_paths(**overrides: object) -> dict:
    base = {
        "model_sql": "models/staging/stg_dimcustomer.sql",
        "model_yaml": "models/staging/_stg_dimcustomer.yml",
    }
    return {**base, **overrides}


def _feedback_item(**overrides: object) -> dict:
    base = {
        "code": "SQL_001",
        "message": "Keywords should be lowercase",
        "severity": "error",
        "ack_required": True,
    }
    return {**base, **overrides}


def _handoff(**overrides: object) -> dict:
    base: dict = {}
    return {**base, **overrides}


def _item_output(**overrides: object) -> dict:
    base = {
        "table_ref": "silver.dimcustomer",
        "model_name": "stg_dimcustomer",
        "artifact_paths": _artifact_paths(),
        "generated": {
            "model_sql": {"materialized": "table", "uses_watermark": False},
            "model_yaml": {
                "has_model_description": True,
                "schema_tests_rendered": ["not_null", "unique"],
                "has_unit_tests": True,
            },
        },
        "execution": {
            "dbt_compile_passed": True,
            "dbt_test_passed": True,
            "self_correction_iterations": 0,
            "dbt_errors": [],
        },
        "review": {"iterations": 1, "verdict": "approved"},
    }
    return {**base, **overrides}


def _generation_output(**overrides: object) -> dict:
    base = {
        "item_id": "silver.dimcustomer",
        "status": "ok",
        "output": _item_output(),
    }
    return {**base, **overrides}


def _check_result(**overrides: object) -> dict:
    base = {"passed": True, "issues": []}
    return {**base, **overrides}


def _review_output(**overrides: object) -> dict:
    base = {
        "item_id": "silver.dimproduct",
        "status": "approved",
        "checks": {
            "standards": _check_result(),
            "correctness": _check_result(),
        },
        "feedback_for_model_generator": [],
        "warnings": [],
        "errors": [],
    }
    return {**base, **overrides}


# ═══════════════════════════════════════════════════════════════════════════
# ModelGeneratorInput
# ═══════════════════════════════════════════════════════════════════════════


class TestModelGeneratorInput:
    def test_valid(self) -> None:
        m = ModelGeneratorInput.model_validate(_generator_input())
        assert m.schema_version == "2.0"
        assert len(m.items) == 1
        assert m.items[0].item_id == "silver.dimcustomer"

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ModelGeneratorInput.model_validate(_generator_input(extra="bad"))

    def test_missing_run_id(self) -> None:
        data = _generator_input()
        del data["run_id"]
        with pytest.raises(ValidationError, match="run_id"):
            ModelGeneratorInput.model_validate(data)

    def test_empty_items_rejected(self) -> None:
        with pytest.raises(ValidationError, match="items"):
            ModelGeneratorInput.model_validate(_generator_input(items=[]))

    def test_wrong_schema_version(self) -> None:
        with pytest.raises(ValidationError, match="schema_version"):
            ModelGeneratorInput.model_validate(_generator_input(schema_version="1.0"))

    def test_optional_selected_writer(self) -> None:
        m = ModelGeneratorInput.model_validate(
            _generator_input(items=[_generator_item(selected_writer=None)])
        )
        assert m.items[0].selected_writer is None

    def test_generator_item_extra_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            GeneratorItem.model_validate(_generator_item(bonus="x"))


# ═══════════════════════════════════════════════════════════════════════════
# ModelGenerationHandoff
# ═══════════════════════════════════════════════════════════════════════════


class TestModelGenerationHandoff:
    def test_minimal(self) -> None:
        m = ModelGenerationHandoff.model_validate(_handoff())
        assert m.artifact_paths is None
        assert m.revision_feedback is None

    def test_full(self) -> None:
        m = ModelGenerationHandoff.model_validate(
            _handoff(
                artifact_paths=_artifact_paths(),
                revision_feedback=[_feedback_item()],
            )
        )
        assert m.artifact_paths is not None
        assert len(m.revision_feedback) == 1

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ModelGenerationHandoff.model_validate(_handoff(extra="bad"))


# ═══════════════════════════════════════════════════════════════════════════
# ModelGenerationOutput
# ═══════════════════════════════════════════════════════════════════════════


class TestModelGenerationOutput:
    def test_valid(self) -> None:
        m = ModelGenerationOutput.model_validate(_generation_output())
        assert m.item_id == "silver.dimcustomer"
        assert m.status == "ok"
        assert m.output.model_name == "stg_dimcustomer"
        assert m.output.generated.model_sql.materialized == "table"
        assert m.output.execution.dbt_compile_passed is True
        assert m.output.review.verdict == "approved"

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ModelGenerationOutput.model_validate(_generation_output(extra="bad"))

    def test_invalid_status(self) -> None:
        with pytest.raises(ValidationError, match="status"):
            ModelGenerationOutput.model_validate(_generation_output(status="success"))

    def test_invalid_review_verdict(self) -> None:
        data = _generation_output()
        data["output"]["review"]["verdict"] = "rejected"
        with pytest.raises(ValidationError, match="verdict"):
            ModelGenerationOutput.model_validate(data)

    def test_negative_iterations_rejected(self) -> None:
        data = _generation_output()
        data["output"]["execution"]["self_correction_iterations"] = -1
        with pytest.raises(ValidationError, match="self_correction_iterations"):
            ModelGenerationOutput.model_validate(data)

    def test_with_warnings_and_errors(self) -> None:
        data = _generation_output()
        data["output"]["warnings"] = [
            {"code": "EQUIVALENCE_GAP", "message": "Missing column", "severity": "warning"}
        ]
        data["output"]["errors"] = [
            {"code": "DBT_COMPILE_FAIL", "message": "Compile failed", "severity": "error"}
        ]
        m = ModelGenerationOutput.model_validate(data)
        assert len(m.output.warnings) == 1
        assert len(m.output.errors) == 1

    def test_partial_status(self) -> None:
        m = ModelGenerationOutput.model_validate(_generation_output(status="partial"))
        assert m.status == "partial"

    def test_approved_with_warnings_verdict(self) -> None:
        data = _generation_output()
        data["output"]["review"]["verdict"] = "approved_with_warnings"
        m = ModelGenerationOutput.model_validate(data)
        assert m.output.review.verdict == "approved_with_warnings"


# ═══════════════════════════════════════════════════════════════════════════
# ModelReviewOutput
# ═══════════════════════════════════════════════════════════════════════════


class TestModelReviewOutput:
    def test_approved(self) -> None:
        m = ModelReviewOutput.model_validate(_review_output())
        assert m.status == "approved"
        assert m.checks.standards.passed is True
        assert m.checks.correctness.passed is True

    def test_revision_requested(self) -> None:
        data = _review_output(
            status="revision_requested",
            feedback_for_model_generator=[_feedback_item()],
        )
        data["checks"]["correctness"] = _check_result(
            passed=False,
            issues=[{"code": "MISSING_SOURCE", "message": "Missing table", "severity": "error"}],
        )
        m = ModelReviewOutput.model_validate(data)
        assert m.status == "revision_requested"
        assert not m.checks.correctness.passed
        assert len(m.feedback_for_model_generator) == 1
        assert m.feedback_for_model_generator[0].code == "SQL_001"

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ModelReviewOutput.model_validate(_review_output(extra="bad"))

    def test_invalid_status(self) -> None:
        with pytest.raises(ValidationError, match="status"):
            ModelReviewOutput.model_validate(_review_output(status="rejected"))

    def test_missing_checks(self) -> None:
        data = _review_output()
        del data["checks"]
        with pytest.raises(ValidationError, match="checks"):
            ModelReviewOutput.model_validate(data)

    def test_with_acknowledgements(self) -> None:
        m = ModelReviewOutput.model_validate(
            _review_output(
                acknowledgements={"SQL_001": "fixed", "CTE_002": "ignored: legacy naming"},
            )
        )
        assert m.acknowledgements is not None
        assert m.acknowledgements["SQL_001"] == "fixed"

    def test_no_acknowledgements_by_default(self) -> None:
        m = ModelReviewOutput.model_validate(_review_output())
        assert m.acknowledgements is None

    def test_with_diagnostics(self) -> None:
        data = _review_output(
            warnings=[{"code": "STYLE_WARN", "message": "Minor issue", "severity": "warning"}],
            errors=[{"code": "FATAL_ERR", "message": "Critical", "severity": "error"}],
        )
        m = ModelReviewOutput.model_validate(data)
        assert len(m.warnings) == 1
        assert len(m.errors) == 1


# ═══════════════════════════════════════════════════════════════════════════
# Shared nested types
# ═══════════════════════════════════════════════════════════════════════════


class TestFeedbackItem:
    def test_valid(self) -> None:
        m = FeedbackItem.model_validate(_feedback_item())
        assert m.code == "SQL_001"
        assert m.ack_required is True

    def test_info_severity(self) -> None:
        m = FeedbackItem.model_validate(_feedback_item(severity="info", ack_required=False))
        assert m.severity == "info"
        assert m.ack_required is False

    def test_invalid_severity(self) -> None:
        with pytest.raises(ValidationError, match="severity"):
            FeedbackItem.model_validate(_feedback_item(severity="critical"))

    def test_extra_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            FeedbackItem.model_validate(_feedback_item(extra="bad"))


class TestArtifactPaths:
    def test_valid(self) -> None:
        m = ArtifactPaths.model_validate(_artifact_paths())
        assert m.model_sql == "models/staging/stg_dimcustomer.sql"

    def test_extra_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ArtifactPaths.model_validate(_artifact_paths(extra="bad"))


class TestCheckResult:
    def test_passed_no_issues(self) -> None:
        m = CheckResult.model_validate({"passed": True, "issues": []})
        assert m.passed is True

    def test_failed_with_issues(self) -> None:
        m = CheckResult.model_validate({
            "passed": False,
            "issues": [{"code": "X", "message": "Y", "severity": "error"}],
        })
        assert not m.passed
        assert len(m.issues) == 1

    def test_extra_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            CheckResult.model_validate({"passed": True, "issues": [], "extra": "bad"})


# ═══════════════════════════════════════════════════════════════════════════
# RenderUnitTestsOutput
# ═══════════════════════════════════════════════════════════════════════════


class TestRenderUnitTestsOutput:
    def test_valid(self) -> None:
        m = RenderUnitTestsOutput.model_validate({
            "tests_rendered": 5,
            "model_name": "stg_dimcustomer",
        })
        assert m.tests_rendered == 5
        assert m.model_name == "stg_dimcustomer"
        assert m.warnings == []
        assert m.errors == []

    def test_with_warnings(self) -> None:
        m = RenderUnitTestsOutput.model_validate({
            "tests_rendered": 0,
            "model_name": "stg_dimcustomer",
            "warnings": [{"code": "NO_UNIT_TESTS", "message": "empty", "severity": "warning"}],
        })
        assert len(m.warnings) == 1

    def test_extra_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            RenderUnitTestsOutput.model_validate({
                "tests_rendered": 1,
                "model_name": "x",
                "extra": "bad",
            })

    def test_negative_count_rejected(self) -> None:
        with pytest.raises(ValidationError, match="tests_rendered"):
            RenderUnitTestsOutput.model_validate({
                "tests_rendered": -1,
                "model_name": "x",
            })
