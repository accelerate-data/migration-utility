"""Tests for enriched catalog section support module exports."""

from __future__ import annotations


def test_enrichment_support_exports_refactor_and_generation_models() -> None:
    from shared.catalog_model_support.enrichment import (
        CompareSqlSummary,
        GenerateSection,
        RefactorSection,
        SemanticCheck,
        SemanticChecks,
        SemanticReview,
        TestGenSection,
    )

    checks = SemanticChecks(
        source_tables=SemanticCheck(passed=True, summary="ok"),
        output_columns=SemanticCheck(passed=True, summary="ok"),
        joins=SemanticCheck(passed=True, summary="ok"),
        filters=SemanticCheck(passed=True, summary="ok"),
        aggregation_grain=SemanticCheck(passed=True, summary="ok"),
    )
    review = SemanticReview(passed=True, checks=checks)
    compare = CompareSqlSummary(
        required=True,
        executed=True,
        passed=True,
        scenarios_total=1,
        scenarios_passed=1,
    )
    refactor = RefactorSection(status="ok", semantic_review=review, compare_sql=compare)

    assert refactor.semantic_review.passed is True
    assert refactor.compare_sql.scenarios_passed == 1
    assert TestGenSection(status="ok").warnings == []
    assert GenerateSection(status="ok").errors == []
