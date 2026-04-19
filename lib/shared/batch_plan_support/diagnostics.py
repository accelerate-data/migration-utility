"""Catalog diagnostic aggregation for batch-plan output."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.diagnostic_reviews import partition_reviewed_warnings
from shared.output_models.dry_run import CatalogDiagnosticEntry


def _collect_catalog_diagnostics(
    project_root: Path,
    inputs: Any,
) -> tuple[list[CatalogDiagnosticEntry], list[CatalogDiagnosticEntry], dict[str, int], int]:
    """Partition catalog diagnostics into visible errors/warnings and hidden counts."""
    all_errors: list[CatalogDiagnosticEntry] = []
    all_warnings: list[CatalogDiagnosticEntry] = []
    resolved_warning_counts: dict[str, int] = {}
    reviewed_warnings_hidden = 0
    for fqn in sorted(inputs.obj_diagnostics):
        object_type = inputs.obj_type_map[fqn]
        visible_warnings, hidden_count = partition_reviewed_warnings(
            project_root,
            fqn=fqn,
            object_type=object_type,
            warnings=[
                diagnostic
                for diagnostic in inputs.obj_diagnostics[fqn]
                if diagnostic.get("severity") != "error"
            ],
        )
        reviewed_warnings_hidden += hidden_count
        resolved_warning_counts[fqn] = hidden_count
        visible_warning_keys = {
            (
                warning.get("code"),
                warning.get("message"),
                warning.get("severity", "warning"),
                warning.get("item_id"),
                warning.get("field"),
            )
            for warning in visible_warnings
        }
        for diagnostic in inputs.obj_diagnostics[fqn]:
            entry = CatalogDiagnosticEntry(fqn=fqn, object_type=object_type, **diagnostic)
            if diagnostic.get("severity") == "error":
                all_errors.append(entry)
            elif (
                diagnostic.get("code"),
                diagnostic.get("message"),
                diagnostic.get("severity", "warning"),
                diagnostic.get("item_id"),
                diagnostic.get("field"),
            ) in visible_warning_keys:
                all_warnings.append(entry)
    return all_errors, all_warnings, resolved_warning_counts, reviewed_warnings_hidden
