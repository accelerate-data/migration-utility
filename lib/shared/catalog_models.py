"""Compatibility facade for catalog Pydantic contract models."""

from __future__ import annotations

from shared.catalog_model_support.base import _CATALOG_CONFIG, _STRICT_CONFIG
from shared.catalog_model_support.catalogs import (
    FunctionCatalog,
    ProcedureCatalog,
    TableCatalog,
    ViewCatalog,
)
from shared.catalog_model_support.diagnostics import DiagnosticsEntry, ProfileDiagnosticsEntry
from shared.catalog_model_support.enrichment import (
    CompareSqlSummary,
    GenerateSection,
    RefactorSection,
    SemanticCheck,
    SemanticChecks,
    SemanticReview,
    TestGenSection,
)
from shared.catalog_model_support.profile import (
    ForeignKeyType,
    PiiSuggestedAction,
    PrimaryKeyType,
    ProfileClassification,
    ProfileForeignKey,
    ProfileNaturalKey,
    ProfilePiiAction,
    ProfilePrimaryKey,
    ProfileSource,
    ProfileWatermark,
    TableProfileSection,
    TableProfileStatus,
    TableResolvedKind,
    ViewClassification,
    ViewProfileSection,
    ViewProfileSource,
    ViewProfileStatus,
)
from shared.catalog_model_support.references import (
    RefEntry,
    ReferencedByBucket,
    ReferencesBucket,
    ScopedRefList,
)
from shared.catalog_model_support.scoping import (
    CandidateWriter,
    ScopingResultItem,
    ScopingSummary,
    ScopingSummaryCounts,
    SqlElement,
    TableScopingSection,
    ViewScopingSection,
)
from shared.catalog_model_support.statements import StatementEntry

__all__ = [
    "_CATALOG_CONFIG",
    "_STRICT_CONFIG",
    "CandidateWriter",
    "CompareSqlSummary",
    "DiagnosticsEntry",
    "ForeignKeyType",
    "FunctionCatalog",
    "GenerateSection",
    "PiiSuggestedAction",
    "PrimaryKeyType",
    "ProcedureCatalog",
    "ProfileClassification",
    "ProfileDiagnosticsEntry",
    "ProfileForeignKey",
    "ProfileNaturalKey",
    "ProfilePiiAction",
    "ProfilePrimaryKey",
    "ProfileSource",
    "ProfileWatermark",
    "RefEntry",
    "ReferencedByBucket",
    "ReferencesBucket",
    "RefactorSection",
    "ScopedRefList",
    "ScopingResultItem",
    "ScopingSummary",
    "ScopingSummaryCounts",
    "SemanticCheck",
    "SemanticChecks",
    "SemanticReview",
    "SqlElement",
    "StatementEntry",
    "TableCatalog",
    "TableProfileSection",
    "TableProfileStatus",
    "TableResolvedKind",
    "TableScopingSection",
    "TestGenSection",
    "ViewCatalog",
    "ViewClassification",
    "ViewProfileSection",
    "ViewProfileSource",
    "ViewProfileStatus",
    "ViewScopingSection",
]
