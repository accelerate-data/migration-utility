"""refactor.py -- Refactoring context assembly, catalog write-back, and diff logic.

Standalone CLI with two subcommands:

    context  Assemble all deterministic context needed for LLM SQL refactoring.
    write    Validate and merge a refactor section into the writer procedure's catalog.

Also exposes ``symmetric_diff`` for comparing two row-dict lists.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain/validation failure
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer

from shared.catalog import (
    load_and_merge_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
    write_json as _write_catalog_json,
)
from shared.context_helpers import (
    collect_source_tables,
    collect_view_source_tables,
    load_object_columns,
    load_proc_body,
    load_proc_statements,
    load_table_columns,
    load_table_profile,
    load_test_spec,
    sandbox_metadata,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
)
from shared.catalog_models import RefactorSection
from shared.cli_utils import emit
from shared.output_models import (
    CompareSqlOutput,
    RefactorContextOutput,
    RefactorWriteOutput,
)
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Constants ────────────────────────────────────────────────────────────────

REFACTOR_STATUSES = frozenset({"ok", "partial", "error"})
WRITE_KEYWORDS = ("insert ", "update ", "delete ", "merge ", "exec ", "create ", "alter ", "drop ")


# ── Symmetric diff ───────────────────────────────────────────────────────────


def _row_to_key(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """Convert a row dict to a hashable key for multiset comparison.

    All values are stringified to handle type mismatches (e.g. Decimal vs str).
    """
    return tuple(sorted((k, str(v)) for k, v in row.items()))


def symmetric_diff(
    rows_a: list[dict[str, Any]],
    rows_b: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute the symmetric difference of two row-dict lists.

    Uses multiset (Counter) comparison to correctly handle duplicate rows.

    Returns::

        {
            "equivalent": bool,
            "a_minus_b": list[dict],  # rows in A but not in B
            "b_minus_a": list[dict],  # rows in B but not in A
            "a_count": int,
            "b_count": int,
        }
    """
    keys_a = [_row_to_key(r) for r in rows_a]
    keys_b = [_row_to_key(r) for r in rows_b]

    counter_a = Counter(keys_a)
    counter_b = Counter(keys_b)

    # Multiset difference: elements in A not accounted for in B
    a_minus_b_counter = counter_a - counter_b
    b_minus_a_counter = counter_b - counter_a

    # Reconstruct row dicts from keys
    def _keys_to_rows(counter: Counter[tuple[tuple[str, str], ...]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, count in counter.items():
            row = dict(key)
            for _ in range(count):
                rows.append(row)
        return rows

    a_minus_b = _keys_to_rows(a_minus_b_counter)
    b_minus_a = _keys_to_rows(b_minus_a_counter)

    return {
        "equivalent": len(a_minus_b) == 0 and len(b_minus_a) == 0,
        "a_minus_b": a_minus_b,
        "b_minus_a": b_minus_a,
        "a_count": len(rows_a),
        "b_count": len(rows_b),
    }


# ── Context assembly ─────────────────────────────────────────────────────────


def _run_context_view(
    project_root: Path,
    fqn_norm: str,
    cat: Any,
) -> RefactorContextOutput:
    """Assemble refactoring context for a view or materialized view."""
    view_sql = cat.sql
    if not view_sql:
        raise ValueError(f"View catalog for {fqn_norm} has no 'sql' key")

    columns = cat.columns
    profile = cat.profile
    if profile is None:
        raise ValueError(f"View catalog for {fqn_norm} has no 'profile' section — run /profile first")

    source_tables = collect_view_source_tables(project_root, fqn_norm)

    object_type = "mv" if cat.is_materialized_view else "view"
    test_spec = load_test_spec(project_root, fqn_norm)
    sandbox = sandbox_metadata(project_root)

    logger.info(
        "event=context_assembled object_type=%s table=%s source_tables=%d",
        object_type, fqn_norm, len(source_tables),
    )

    return RefactorContextOutput(
        table=fqn_norm,
        object_type=object_type,
        view_sql=view_sql,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        columns=columns,
        source_tables=source_tables,
        test_spec=test_spec,
        sandbox=sandbox,
    )


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> RefactorContextOutput:
    """Assemble refactoring context for a table, view, or materialized view.

    Auto-detects object type from catalog presence:
    - If ``catalog/views/<fqn>.json`` exists → view path (no writer needed)
    - Otherwise → table path (requires writer procedure)

    For tables, if *writer_fqn* is not provided, reads
    ``scoping.selected_writer`` from the table catalog.
    """
    fqn_norm = normalize(table_fqn)

    # Auto-detect: view/MV takes precedence when no explicit writer is given
    if not writer_fqn:
        view_cat = load_view_catalog(project_root, fqn_norm)
        if view_cat is not None:
            return _run_context_view(project_root, fqn_norm, view_cat)

    # Table path
    if not writer_fqn:
        writer_fqn = read_selected_writer(project_root, fqn_norm)
        if not writer_fqn:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {fqn_norm}"
            )
    writer_norm = normalize(writer_fqn)

    proc_cat = load_proc_catalog(project_root, writer_norm)
    table_slices = (proc_cat.table_slices or {}) if proc_cat else {}
    writer_ddl_slice = table_slices.get(fqn_norm) or None

    profile = load_table_profile(project_root, fqn_norm)
    statements = load_proc_statements(project_root, writer_norm)
    proc_body = load_proc_body(project_root, writer_norm)
    columns = load_table_columns(project_root, fqn_norm)
    source_tables = collect_source_tables(project_root, writer_norm)
    source_columns = {
        fqn: load_object_columns(project_root, fqn) for fqn in source_tables
    }
    test_spec = load_test_spec(project_root, fqn_norm)
    sandbox = sandbox_metadata(project_root)

    logger.info(
        "event=context_assembled table=%s writer=%s source_tables=%d test_scenarios=%d",
        fqn_norm, writer_norm, len(source_tables),
        len(test_spec.get("unit_tests", [])) if test_spec else 0,
    )

    return RefactorContextOutput(
        table=fqn_norm,
        writer=writer_norm,
        proc_body=proc_body,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        statements=statements,
        columns=columns,
        source_tables=source_tables,
        source_columns=source_columns,
        test_spec=test_spec,
        sandbox=sandbox,
        writer_ddl_slice=writer_ddl_slice,
    )


# ── Write validation and merge ───────────────────────────────────────────────


def _validate_refactor(refactor: dict[str, Any]) -> list[str]:
    """Validate a refactor dict. Returns a list of error messages (empty = valid)."""
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
    """Normalize semantic-review payload from the LLM sub-agent."""
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
    """Reduce compare-sql output to the persisted proof summary."""
    if compare_sql_result is None:
        return {
            "required": compare_required,
            "executed": False,
            "passed": False,
            "scenarios_total": 0,
            "scenarios_passed": 0,
            "failed_scenarios": [],
        }

    # Validate input shape from test-harness compare-sql
    validated = CompareSqlOutput.model_validate(compare_sql_result)

    failed_scenarios = [
        r.scenario_name
        for r in validated.results
        if r.status != "ok" or r.equivalent is False
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
    """Derive persisted refactor status from proof evidence."""
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

    # Harness/logical-only mode never upgrades to ok.
    return "partial"


def run_write(
    project_root: Path,
    table_fqn: str,
    extracted_sql: str,
    refactored_sql: str,
    semantic_review: dict[str, Any] | None = None,
    compare_sql_result: dict[str, Any] | None = None,
    compare_required: bool = True,
) -> RefactorWriteOutput:
    """Validate and merge a refactor section into the catalog.

    Auto-detects object type:
    - If ``catalog/views/<fqn>.json`` exists → writes refactor block to view catalog
    - Otherwise → resolves writer from table catalog, writes to procedure catalog

    Status is derived from persisted proof evidence:
    - ``ok`` only when semantic review passes and executable compare passes
    - ``partial`` when only logical review exists, executable compare is skipped,
      or unresolved equivalence issues remain
    - ``error`` when no usable SQL exists

    Returns a confirmation dict on success.
    Raises ValueError on validation failure, OSError/json.JSONDecodeError on IO error.
    """
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

    # Auto-detect: check view catalog first
    view_cat_model = load_view_catalog(project_root, table_norm)
    if view_cat_model is not None:
        RefactorSection.model_validate(refactor_data)
        return _run_write_view(project_root, table_norm, refactor_data)

    # Table path: resolve writer from table catalog
    writer_fqn = read_selected_writer(project_root, table_norm)
    if not writer_fqn:
        raise ValueError(
            f"No scoping.selected_writer in table catalog for {table_norm}"
        )
    writer_norm = normalize(writer_fqn)

    # Load existing procedure catalog file
    catalog_path = resolve_catalog_dir(project_root) / "procedures" / f"{writer_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("procedure", writer_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    except OSError as exc:
        logger.error(
            "event=write_failed operation=read_catalog table=%s writer=%s error=%s",
            table_norm, writer_norm, exc,
        )
        raise

    # Validate refactor section through Pydantic model
    RefactorSection.model_validate(refactor_data)

    # Merge refactor section onto procedure catalog
    existing["refactor"] = refactor_data

    try:
        _write_catalog_json(catalog_path, existing)
    except OSError as exc:
        logger.error(
            "event=write_failed operation=atomic_write table=%s writer=%s error=%s",
            table_norm, writer_norm, exc,
        )
        raise

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


def _run_write_view(
    project_root: Path,
    fqn_norm: str,
    refactor_data: dict[str, Any],
) -> RefactorWriteOutput:
    """Write refactor block to a view catalog file."""
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


# ── CLI commands ─────────────────────────────────────────────────────────────


@app.command()
def context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
) -> None:
    """Assemble refactoring context for a table + writer pair."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_context(project_root, table, writer)
    except CatalogFileMissingError as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (ValueError, FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    extracted_sql: str = typer.Option("", help="Extracted core SQL string"),
    extracted_sql_file: Optional[Path] = typer.Option(None, "--extracted-sql-file", help="Path to file containing extracted core SQL"),
    refactored_sql: str = typer.Option("", help="Refactored SQL string"),
    refactored_sql_file: Optional[Path] = typer.Option(None, "--refactored-sql-file", help="Path to file containing refactored SQL"),
    semantic_review_file: Optional[Path] = typer.Option(None, "--semantic-review-file", help="Path to JSON file containing structured semantic review evidence"),
    compare_sql_file: Optional[Path] = typer.Option(None, "--compare-sql-file", help="Path to JSON file containing compare-sql output"),
    compare_required: bool = typer.Option(True, "--compare-required/--no-compare-required", help="Require executable compare-sql proof for status=ok"),
) -> None:
    """Validate and merge a refactor section into the writer procedure's catalog."""
    if extracted_sql_file:
        extracted_sql = extracted_sql_file.read_text(encoding="utf-8")
    if refactored_sql_file:
        refactored_sql = refactored_sql_file.read_text(encoding="utf-8")
    semantic_review = None
    compare_sql_result = None
    if semantic_review_file:
        semantic_review = json.loads(semantic_review_file.read_text(encoding="utf-8"))
    if compare_sql_file:
        compare_sql_result = json.loads(compare_sql_file.read_text(encoding="utf-8"))
    if not extracted_sql and not refactored_sql:
        logger.error("event=write_failed table=%s error=no SQL provided", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)

    try:
        result = run_write(
            project_root,
            table,
            extracted_sql,
            refactored_sql,
            semantic_review=semantic_review,
            compare_sql_result=compare_sql_result,
            compare_required=compare_required,
        )
    except (ValueError, CatalogFileMissingError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit(RefactorWriteOutput(ok=False, error=str(exc), table=normalize(table)))
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit(RefactorWriteOutput(ok=False, error=str(exc), table=normalize(table)))
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
