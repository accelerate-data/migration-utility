"""Write and validation helpers for refactor."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_and_merge_catalog, load_view_catalog, read_selected_writer, write_json as _write_catalog_json
from shared.catalog_models import RefactorSection
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.refactor import RefactorWriteOutput
from shared.output_models.sandbox import CompareSqlOutput

logger = logging.getLogger(__name__)

WRITE_KEYWORDS = ("insert ", "update ", "delete ", "merge ", "exec ", "create ", "alter ", "drop ")


def _validate_refactor(refactor: dict[str, Any]) -> list[str]:
    """Validate a refactor dict."""
    errors: list[str] = []
    extracted_sql = (refactor.get("extracted_sql") or "").lower()
    refactored_sql = (refactor.get("refactored_sql") or "").lower()
    for keyword in WRITE_KEYWORDS:
        if extracted_sql and keyword in extracted_sql:
            errors.append(f"extracted_sql must be a pure SELECT and cannot contain '{keyword.strip()}'")
        if refactored_sql and keyword in refactored_sql:
            errors.append(f"refactored_sql must be a pure SELECT and cannot contain '{keyword.strip()}'")
    return errors


def _normalize_semantic_review(semantic_review: dict[str, Any] | None) -> dict[str, Any] | None:
    if semantic_review is None:
        return None
    checks = semantic_review.get("checks") or {}
    return {
        "passed": bool(semantic_review.get("passed")),
        "checks": {
            "source_tables": {
                "passed": bool((checks.get("source_tables") or {}).get("passed")),
                "summary": str((checks.get("source_tables") or {}).get("summary") or ""),
            },
            "output_columns": {
                "passed": bool((checks.get("output_columns") or {}).get("passed")),
                "summary": str((checks.get("output_columns") or {}).get("summary") or ""),
            },
            "joins": {
                "passed": bool((checks.get("joins") or {}).get("passed")),
                "summary": str((checks.get("joins") or {}).get("summary") or ""),
            },
            "filters": {
                "passed": bool((checks.get("filters") or {}).get("passed")),
                "summary": str((checks.get("filters") or {}).get("summary") or ""),
            },
            "aggregation_grain": {
                "passed": bool((checks.get("aggregation_grain") or {}).get("passed")),
                "summary": str((checks.get("aggregation_grain") or {}).get("summary") or ""),
            },
        },
        "issues": list(semantic_review.get("issues") or []),
    }


def _summarize_compare_sql(compare_sql_result: dict[str, Any] | None, compare_required: bool) -> dict[str, Any]:
    if compare_sql_result is None:
        return {
            "required": compare_required,
            "executed": False,
            "passed": False,
            "scenarios_total": 0,
            "scenarios_passed": 0,
            "failed_scenarios": [],
        }
    validated = CompareSqlOutput.model_validate(compare_sql_result)
    failed_scenarios = [
        result.scenario_name
        for result in validated.results
        if result.status != "ok" or result.equivalent is False
    ]
    return {
        "required": compare_required,
        "executed": True,
        "passed": len(failed_scenarios) == 0 and validated.total > 0,
        "scenarios_total": validated.total,
        "scenarios_passed": validated.passed,
        "failed_scenarios": failed_scenarios,
    }


def _derive_refactor_status(
    extracted_sql: str,
    refactored_sql: str,
    semantic_review: dict[str, Any] | None,
    compare_sql: dict[str, Any],
) -> str:
    extracted_stripped = extracted_sql.strip()
    refactored_stripped = refactored_sql.strip()
    if not extracted_stripped and not refactored_stripped:
        return "error"
    if not extracted_stripped or not refactored_stripped:
        return "partial"
    semantic_passed = bool((semantic_review or {}).get("passed"))
    if compare_sql["required"]:
        if semantic_passed and compare_sql["executed"] and compare_sql["passed"]:
            return "ok"
        return "partial"
    return "partial"


def _run_write_view(project_root: Path, fqn_norm: str, refactor_data: dict[str, Any]) -> RefactorWriteOutput:
    result = load_and_merge_catalog(project_root, fqn_norm, "refactor", refactor_data)
    logger.info(
        "event=write_complete object_type=view view=%s catalog_path=%s",
        fqn_norm, result["catalog_path"],
    )
    return RefactorWriteOutput(
        ok=result["ok"],
        table=result["table"],
        status=result.get("status"),
        catalog_path=result["catalog_path"],
        object_type="view",
    )


def run_write(
    project_root: Path,
    table_fqn: str,
    extracted_sql: str,
    refactored_sql: str,
    semantic_review: dict[str, Any] | None = None,
    compare_sql_result: dict[str, Any] | None = None,
    compare_required: bool = True,
    *,
    catalog_dir: Path,
) -> RefactorWriteOutput:
    """Validate and merge a refactor section into the catalog."""
    table_norm = normalize(table_fqn)
    normalized_semantic_review = _normalize_semantic_review(semantic_review)
    compare_sql_summary = _summarize_compare_sql(compare_sql_result, compare_required)
    status = _derive_refactor_status(
        extracted_sql=extracted_sql,
        refactored_sql=refactored_sql,
        semantic_review=normalized_semantic_review,
        compare_sql=compare_sql_summary,
    )

    refactor_data: dict[str, Any] = {
        "status": status,
        "extracted_sql": " ".join(extracted_sql.split()),
        "refactored_sql": " ".join(refactored_sql.split()),
        "semantic_review": normalized_semantic_review,
        "compare_sql": compare_sql_summary,
    }
    errors = _validate_refactor(refactor_data)
    if errors:
        raise ValueError(f"Refactor validation failed for {table_norm}: {'; '.join(errors)}")

    if load_view_catalog(project_root, table_norm) is not None:
        RefactorSection.model_validate(refactor_data)
        return _run_write_view(project_root, table_norm, refactor_data)

    writer_fqn = read_selected_writer(project_root, table_norm)
    if not writer_fqn:
        raise ValueError(f"No scoping.selected_writer in table catalog for {table_norm}")
    writer_norm = normalize(writer_fqn)
    catalog_path = catalog_dir / "procedures" / f"{writer_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("procedure", writer_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc

    RefactorSection.model_validate(refactor_data)
    existing["refactor"] = refactor_data
    _write_catalog_json(catalog_path, existing)
    logger.info(
        "event=write_complete table=%s writer=%s catalog_path=%s",
        table_norm, writer_norm, catalog_path,
    )
    return RefactorWriteOutput(
        ok=True,
        table=table_norm,
        status=status,
        writer=writer_norm,
        catalog_path=str(catalog_path),
    )
