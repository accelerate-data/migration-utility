"""Compatibility barrel for shared CLI output models."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from shared.output_models.shared import OUTPUT_CONFIG

_MODEL_MODULES = {
    "CatalogEnrichOutput": "catalog_enrich",
    "AnalysisError": "discover",
    "BasicRefs": "discover",
    "ColumnDef": "discover",
    "DiscoverListOutput": "discover",
    "DiscoverRefsOutput": "discover",
    "DiscoverShowOutput": "discover",
    "ParamDef": "discover",
    "ProcRefs": "discover",
    "SqlElement": "discover",
    "StatementEntry": "discover",
    "WriterEntry": "discover",
    "BatchPlanOutput": "dry_run",
    "BatchSummary": "dry_run",
    "CatalogDiagnosticEntry": "dry_run",
    "CatalogDiagnostics": "dry_run",
    "CircularRef": "dry_run",
    "DiagnosticEntry": "dry_run",
    "DryRunOutput": "dry_run",
    "ExcludeOutput": "dry_run",
    "ExcludedObject": "dry_run",
    "GuardResult": "dry_run",
    "MigrateBatch": "dry_run",
    "NaObject": "dry_run",
    "ObjectNode": "dry_run",
    "ObjectStatus": "dry_run",
    "SourcePending": "dry_run",
    "SourceTable": "dry_run",
    "SeedTable": "dry_run",
    "StageStatuses": "dry_run",
    "StatusOutput": "dry_run",
    "StatusDiagnosticRow": "dry_run",
    "StatusNextAction": "dry_run",
    "StatusPipelineRow": "dry_run",
    "StatusSummaryDashboard": "dry_run",
    "StatusSummary": "dry_run",
    "SyncExcludedWarningsOutput": "dry_run",
    "GenerateSourcesOutput": "generate_sources",
    "SetupTargetOutput": "target_setup",
    "ScaffoldHooksOutput": "init",
    "ScaffoldProjectOutput": "init",
    "MigrateContextOutput": "migrate",
    "MigrateWriteGenerateOutput": "migrate",
    "MigrateWriteOutput": "migrate",
    "RenderUnitTestsOutput": "migrate",
    "ArtifactPaths": "model_generation",
    "CheckResult": "model_generation",
    "ExecutionInfo": "model_generation",
    "FeedbackItem": "model_generation",
    "GeneratedInfo": "model_generation",
    "GeneratedModelInfo": "model_generation",
    "GeneratedYamlInfo": "model_generation",
    "GeneratorItem": "model_generation",
    "ModelGenerationHandoff": "model_generation",
    "ModelGenerationItemOutput": "model_generation",
    "ModelGenerationOutput": "model_generation",
    "ModelGeneratorInput": "model_generation",
    "ModelReviewOutput": "model_generation",
    "ReviewChecks": "model_generation",
    "ReviewInfo": "model_generation",
    "AutoIncrementSignal": "profile",
    "CatalogSignals": "profile",
    "ChangeCaptureSignal": "profile",
    "EnrichedInScopeRef": "profile",
    "EnrichedScopedRefList": "profile",
    "ForeignKeySignal": "profile",
    "OutOfScopeRef": "profile",
    "PrimaryKeyConstraint": "profile",
    "ProfileColumnDef": "profile",
    "ProfileContext": "profile",
    "RelatedProcedure": "profile",
    "SensitivitySignal": "profile",
    "UniqueIndexSignal": "profile",
    "ViewColumnDef": "profile",
    "ViewProfileContext": "profile",
    "ViewReferencedBy": "profile",
    "ViewReferences": "profile",
    "RefactorContextOutput": "refactor",
    "RefactorWriteOutput": "refactor",
    "ReplicateSourceTablesOutput": "replicate_source_tables",
    "ReplicateTableResult": "replicate_source_tables",
    "CompareSqlOutput": "sandbox",
    "CompareSqlScenario": "sandbox",
    "ErrorEntry": "sandbox",
    "ExecuteSpecOutput": "sandbox",
    "ExecuteSpecResult": "sandbox",
    "SandboxDownOutput": "sandbox",
    "SandboxStatusOutput": "sandbox",
    "SandboxUpOutput": "sandbox",
    "TestHarnessExecuteOutput": "sandbox",
    "BranchEntry": "test_specs",
    "CommandSummary": "test_specs",
    "CoverageSection": "test_specs",
    "ExpectEntry": "test_specs",
    "GeneratorFeedback": "test_specs",
    "GivenEntry": "test_specs",
    "QualityIssue": "test_specs",
    "ReviewerBranchEntry": "test_specs",
    "TestReviewOutput": "test_specs",
    "TestSpec": "test_specs",
    "TestSpecOutput": "test_specs",
    "UnitTestEntry": "test_specs",
    "UncoveredBranch": "test_specs",
    "UntestableBranch": "test_specs",
    "ValidationSection": "test_specs",
    "WriteSliceOutput": "writeback",
    "WriteSeedOutput": "writeback",
    "WriteSourceOutput": "writeback",
}


def __getattr__(name: str) -> Any:
    module_name = _MODEL_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"shared.output_models.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


__all__ = ["OUTPUT_CONFIG", *_MODEL_MODULES.keys()]
