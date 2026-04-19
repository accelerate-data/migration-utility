"""Enrichment, refactor, test generation, and generation catalog section models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import DiagnosticsEntry


class SemanticCheck(BaseModel):
    """A single semantic equivalence check (e.g. source_tables, joins)."""

    model_config = _STRICT_CONFIG

    passed: bool
    summary: str


class SemanticChecks(BaseModel):
    """The five semantic equivalence checks performed during refactoring review."""

    model_config = _STRICT_CONFIG

    source_tables: SemanticCheck
    output_columns: SemanticCheck
    joins: SemanticCheck
    filters: SemanticCheck
    aggregation_grain: SemanticCheck


class SemanticReview(BaseModel):
    """Structured semantic review from the refactoring-sql skill's review sub-agent."""

    model_config = _STRICT_CONFIG

    passed: bool
    checks: SemanticChecks
    issues: list[Any] = []


class CompareSqlSummary(BaseModel):
    """Persisted proof summary from executable compare-sql equivalence testing."""

    model_config = _STRICT_CONFIG

    required: bool
    executed: bool
    passed: bool
    scenarios_total: int
    scenarios_passed: int
    failed_scenarios: list[Any] = []


class RefactorSection(BaseModel):
    """CTE restructuring results from the refactoring-sql skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    extracted_sql: str | None = None
    refactored_sql: str | None = None
    semantic_review: SemanticReview | None = None
    compare_sql: CompareSqlSummary | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


class TestGenSection(BaseModel):
    """Test generation summary from the test-harness write command."""

    model_config = _STRICT_CONFIG

    status: str = ""
    test_spec_path: str | None = None
    branches: int | None = None
    unit_tests: int | None = None
    coverage: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class GenerateSection(BaseModel):
    """dbt model generation summary from the migrate write-catalog command."""

    model_config = _STRICT_CONFIG

    status: str = ""
    model_path: str | None = None
    schema_yml: bool | None = None
    compiled: bool | None = None
    tests_passed: bool | None = None
    test_count: int | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


__all__ = [
    "CompareSqlSummary",
    "GenerateSection",
    "RefactorSection",
    "SemanticCheck",
    "SemanticChecks",
    "SemanticReview",
    "TestGenSection",
]
