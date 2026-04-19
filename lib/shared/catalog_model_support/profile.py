"""Profile aliases and section models for catalog contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import ProfileDiagnosticsEntry

TableProfileStatus = Literal["", "ok", "partial", "error"]
ViewProfileStatus = Literal["ok"] | None
ProfileSource = Literal["catalog", "llm", "catalog+llm", "manual"]
TableResolvedKind = Literal[
    "seed",
    "insert",
    "dim_non_scd",
    "dim_scd1",
    "dim_scd2",
    "dim_junk",
    "fact",
    "fact_insert",
    "fact_transaction",
    "fact_periodic_snapshot",
    "fact_accumulating_snapshot",
    "fact_aggregate",
]
PrimaryKeyType = Literal["surrogate", "natural", "composite", "unknown", "none"]
ForeignKeyType = Literal["standard", "role_playing", "degenerate"]
PiiSuggestedAction = Literal["mask", "drop", "tokenize", "keep"]
ViewClassification = Literal["stg", "mart"]
ViewProfileSource = Literal["llm"]


class ProfileClassification(BaseModel):
    """Table kind classification decision."""

    model_config = _STRICT_CONFIG

    resolved_kind: TableResolvedKind | None = None
    source: ProfileSource | None = None
    confidence: str | None = None
    rationale: str | None = None


class ProfilePrimaryKey(BaseModel):
    """Primary key decision for a profiled table."""

    model_config = _STRICT_CONFIG

    column: str | None = None
    columns: list[str] = Field(default_factory=list)
    primary_key_type: PrimaryKeyType | None = None
    source: ProfileSource | None = None
    rationale: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_column(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if normalized.get("column") and not normalized.get("columns"):
            normalized["columns"] = [normalized["column"]]
        return normalized


class ProfileNaturalKey(BaseModel):
    """Natural key decision for a profiled table."""

    model_config = _STRICT_CONFIG

    columns: list[str] = Field(default_factory=list)
    source: ProfileSource | None = None
    rationale: str | None = None


class ProfileWatermark(BaseModel):
    """Incremental watermark decision for a profiled table."""

    model_config = _STRICT_CONFIG

    column: str | None = None
    columns: list[str] = Field(default_factory=list)
    strategy: str | None = None
    watermark_type: str | None = None
    source: ProfileSource | None = None
    rationale: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_columns(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        columns = normalized.get("columns")
        if not normalized.get("column") and isinstance(columns, list) and columns:
            normalized["column"] = columns[0]
        return normalized


class ProfileForeignKey(BaseModel):
    """Foreign-key role decision for a profiled table column."""

    model_config = _STRICT_CONFIG

    column: str | None = None
    columns: list[str] = Field(default_factory=list)
    fk_type: ForeignKeyType | None = None
    references_source_relation: str | None = None
    references_column: str | None = None
    references_table: str | None = None
    source: ProfileSource | None = None
    rationale: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fk_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        columns = normalized.get("columns")
        if not normalized.get("column") and isinstance(columns, list) and columns:
            normalized["column"] = columns[0]
        if not normalized.get("references_source_relation") and normalized.get("references_table"):
            normalized["references_source_relation"] = normalized["references_table"]
        return normalized


class ProfilePiiAction(BaseModel):
    """PII handling decision for a profiled table column."""

    model_config = _STRICT_CONFIG

    column: str
    suggested_action: PiiSuggestedAction | None = None
    action: PiiSuggestedAction | None = None
    entity: str | None = None
    source: ProfileSource | None = None
    rationale: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_action(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if not normalized.get("suggested_action") and normalized.get("action"):
            normalized["suggested_action"] = normalized["action"]
        return normalized


class TableProfileSection(BaseModel):
    """Profiling results for a table (classification, keys, PII, etc.)."""

    model_config = _STRICT_CONFIG

    status: TableProfileStatus = ""
    classification: ProfileClassification | None = None
    primary_key: ProfilePrimaryKey | None = None
    natural_key: ProfileNaturalKey | None = None
    watermark: ProfileWatermark | None = None
    foreign_keys: list[ProfileForeignKey] = Field(default_factory=list)
    pii_actions: list[ProfilePiiAction] = Field(default_factory=list)
    warnings: list[ProfileDiagnosticsEntry] = Field(default_factory=list)
    errors: list[ProfileDiagnosticsEntry] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_profile_shapes(cls, data: Any) -> Any:
        """Normalize older persisted shorthand shapes into typed profile sections."""
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if isinstance(normalized.get("natural_key"), list):
            normalized["natural_key"] = {"columns": normalized["natural_key"]}
        return normalized


class ViewProfileSection(BaseModel):
    """Profiling results for a view (stg/mart classification)."""

    model_config = _STRICT_CONFIG

    status: ViewProfileStatus = None
    classification: ViewClassification
    rationale: str = ""
    source: ViewProfileSource
    warnings: list[ProfileDiagnosticsEntry] = Field(default_factory=list)
    errors: list[ProfileDiagnosticsEntry] = Field(default_factory=list)


__all__ = [
    "ForeignKeyType",
    "PiiSuggestedAction",
    "PrimaryKeyType",
    "ProfileClassification",
    "ProfileForeignKey",
    "ProfileNaturalKey",
    "ProfilePiiAction",
    "ProfilePrimaryKey",
    "ProfileSource",
    "ProfileWatermark",
    "TableProfileSection",
    "TableProfileStatus",
    "TableResolvedKind",
    "ViewClassification",
    "ViewProfileSection",
    "ViewProfileSource",
    "ViewProfileStatus",
]
