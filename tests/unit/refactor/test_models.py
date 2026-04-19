from __future__ import annotations


import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from shared.catalog_models import (
    CompareSqlSummary,
    RefactorSection,
    SemanticReview,
)
from shared.output_models.refactor import RefactorContextOutput, RefactorWriteOutput
from shared.output_models.sandbox import CompareSqlOutput, CompareSqlScenario
from tests.unit.refactor.helpers import (
    _compare_sql_result,
    _semantic_review,
)

_cli_runner = CliRunner()


class TestCatalogModels:
    """Tests for tightened RefactorSection and related models."""

    def test_semantic_review_valid(self) -> None:
        data = _semantic_review()
        model = SemanticReview.model_validate(data)
        assert model.passed is True
        assert model.checks.source_tables.passed is True
        assert model.checks.aggregation_grain.summary == "aggregation grain matches"

    def test_semantic_review_rejects_extra_field(self) -> None:
        data = _semantic_review()
        data["unexpected_field"] = "boom"
        with pytest.raises(ValidationError):
            SemanticReview.model_validate(data)

    def test_compare_sql_summary_valid(self) -> None:
        model = CompareSqlSummary(
            required=True, executed=True, passed=True,
            scenarios_total=2, scenarios_passed=2,
        )
        assert model.passed is True
        assert model.failed_scenarios == []

    def test_compare_sql_summary_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError):
            CompareSqlSummary(
                required=True, executed=True, passed=True,
                scenarios_total=2, scenarios_passed=2,
                bogus="nope",
            )

    def test_refactor_section_typed_fields(self) -> None:
        section = RefactorSection(
            status="ok",
            extracted_sql="SELECT 1",
            refactored_sql="WITH src AS (SELECT 1) SELECT * FROM src",
            semantic_review=SemanticReview.model_validate(_semantic_review()),
            compare_sql=CompareSqlSummary(
                required=True, executed=True, passed=True,
                scenarios_total=2, scenarios_passed=2,
            ),
        )
        assert section.semantic_review.passed is True
        assert section.compare_sql.scenarios_total == 2

    def test_refactor_section_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError):
            RefactorSection(status="ok", extra_junk="bad")

class TestOutputModels:
    """Tests for CLI output Pydantic models."""

    def test_refactor_context_output_rejects_extra(self) -> None:
        with pytest.raises(ValidationError):
            RefactorContextOutput(
                table="silver.t", profile={}, columns=[], source_tables=[],
                bogus="nope",
            )

    def test_refactor_write_output_success(self) -> None:
        model = RefactorWriteOutput(
            ok=True, table="silver.t", status="ok",
            writer="dbo.usp", catalog_path="/tmp/x.json",
        )
        assert model.ok is True
        assert model.writer == "dbo.usp"

    def test_refactor_write_output_failure(self) -> None:
        model = RefactorWriteOutput(
            ok=False, table="silver.t", error="something broke",
        )
        assert model.ok is False
        assert model.error == "something broke"

    def test_compare_sql_output_valid(self) -> None:
        data = _compare_sql_result()
        model = CompareSqlOutput.model_validate(data)
        assert model.total == 2
        assert len(model.results) == 2
        assert isinstance(model.results[0], CompareSqlScenario)

    def test_compare_sql_output_rejects_extra(self) -> None:
        data = _compare_sql_result()
        data["bogus"] = "nope"
        with pytest.raises(ValidationError):
            CompareSqlOutput.model_validate(data)
