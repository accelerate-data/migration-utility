from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import DiagnosticsEntry

__all__ = [
    "CandidateWriter",
    "ScopingResultItem",
    "ScopingSummary",
    "ScopingSummaryCounts",
    "SqlElement",
    "TableScopingSection",
    "ViewScopingSection",
]


class CandidateWriter(BaseModel):
    """A single candidate writer procedure discovered during table scoping."""

    model_config = _STRICT_CONFIG

    procedure_name: str
    rationale: str
    dependencies: dict[str, Any] | None = None


class TableScopingSection(BaseModel):
    """Writer-selection results from the analyzing-table skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    selected_writer: str | None = None
    selected_writer_rationale: str | None = None
    candidates: list[CandidateWriter] | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


class SqlElement(BaseModel):
    """A single SQL structural element discovered during view scoping."""

    model_config = _STRICT_CONFIG

    type: str
    detail: str


class ViewScopingSection(BaseModel):
    """SQL analysis results from the analyzing-table/view skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    sql_elements: list[SqlElement] | None = None
    call_tree: dict[str, Any] | None = None
    logic_summary: str | None = None
    rationale: str | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


class ScopingResultItem(BaseModel):
    """Per-item status in a scoping batch run."""

    model_config = _STRICT_CONFIG

    item_id: str
    status: Literal[
        "resolved", "ambiguous_multi_writer", "no_writer_found", "analyzed", "error",
    ]


class ScopingSummaryCounts(BaseModel):
    """Aggregate counts for a scoping batch run."""

    model_config = _STRICT_CONFIG

    total: int
    resolved: int
    ambiguous_multi_writer: int
    no_writer_found: int
    analyzed: int
    error: int


class ScopingSummary(BaseModel):
    """Batch rollup written to ``.migration-runs/summary.<epoch>.json`` by the scope command."""

    model_config = _STRICT_CONFIG

    schema_version: Literal["1.0"]
    run_id: str
    results: list[ScopingResultItem]
    summary: ScopingSummaryCounts
