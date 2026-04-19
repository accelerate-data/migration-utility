"""Compatibility tests for the shared.catalog_models facade."""

from __future__ import annotations


def test_catalog_models_facade_exports_current_public_surface() -> None:
    import shared.catalog_models as models

    expected = [
        "_CATALOG_CONFIG",
        "_STRICT_CONFIG",
        "RefEntry",
        "ScopedRefList",
        "ReferencesBucket",
        "ReferencedByBucket",
        "StatementEntry",
        "DiagnosticsEntry",
        "ProfileDiagnosticsEntry",
        "TableProfileStatus",
        "ViewProfileStatus",
        "ProfileSource",
        "TableResolvedKind",
        "PrimaryKeyType",
        "ForeignKeyType",
        "PiiSuggestedAction",
        "ViewClassification",
        "ViewProfileSource",
        "CandidateWriter",
        "TableScopingSection",
        "SqlElement",
        "ViewScopingSection",
        "ScopingResultItem",
        "ScopingSummaryCounts",
        "ScopingSummary",
        "ProfileClassification",
        "ProfilePrimaryKey",
        "ProfileNaturalKey",
        "ProfileWatermark",
        "ProfileForeignKey",
        "ProfilePiiAction",
        "TableProfileSection",
        "ViewProfileSection",
        "SemanticCheck",
        "SemanticChecks",
        "SemanticReview",
        "CompareSqlSummary",
        "RefactorSection",
        "TestGenSection",
        "GenerateSection",
        "TableCatalog",
        "ProcedureCatalog",
        "ViewCatalog",
        "FunctionCatalog",
    ]

    missing = [name for name in expected if not hasattr(models, name)]
    assert missing == []


def test_catalog_models_facade_reexports_support_module_classes_by_identity() -> None:
    import shared.catalog_models as models
    from shared.catalog_model_support.catalogs import TableCatalog
    from shared.catalog_model_support.diagnostics import DiagnosticsEntry
    from shared.catalog_model_support.enrichment import RefactorSection
    from shared.catalog_model_support.profile import TableProfileSection
    from shared.catalog_model_support.references import RefEntry
    from shared.catalog_model_support.scoping import TableScopingSection

    assert models.RefEntry is RefEntry
    assert models.DiagnosticsEntry is DiagnosticsEntry
    assert models.TableScopingSection is TableScopingSection
    assert models.TableProfileSection is TableProfileSection
    assert models.RefactorSection is RefactorSection
    assert models.TableCatalog is TableCatalog
