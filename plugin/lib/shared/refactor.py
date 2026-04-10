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
import time as _time
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

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
from shared.env_config import resolve_catalog_dir, resolve_dbt_project_path, resolve_project_root
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)
_SCHEMA_DIR = Path(__file__).with_name("schemas")


# ── Constants ────────────────────────────────────────────────────────────────

REFACTOR_STATUSES = frozenset({"ok", "partial", "error"})
WRITE_KEYWORDS = ("insert ", "update ", "delete ", "merge ", "exec ", "create ", "alter ", "drop ")


# ── Helpers ──────────────────────────────────────────────────────────────────


_SCHEMA_REGISTRY_CACHE: tuple[dict[str, Any], Registry] | None = None


def _get_schema_registry() -> tuple[dict[str, Any], Registry]:
    """Load all schemas and build a registry, cached for the process lifetime."""
    global _SCHEMA_REGISTRY_CACHE  # noqa: PLW0603
    if _SCHEMA_REGISTRY_CACHE is not None:
        return _SCHEMA_REGISTRY_CACHE
    registry = Registry()
    loaded: dict[str, Any] = {}
    for schema_path in _SCHEMA_DIR.glob("*.json"):
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        loaded[schema_path.name] = schema
        resource = Resource.from_contents(schema)
        registry = registry.with_resource(schema_path.name, resource)
        registry = registry.with_resource(schema_path.resolve().as_uri(), resource)
    _SCHEMA_REGISTRY_CACHE = (loaded, registry)
    return _SCHEMA_REGISTRY_CACHE


def _load_schema_with_store(schema_name: str) -> tuple[dict[str, Any], Registry]:
    """Load a schema file and registry for local schema references."""
    loaded, registry = _get_schema_registry()
    return loaded[schema_name], registry


def _format_validation_errors(errors: list[Any]) -> str:
    """Render jsonschema validation errors in compact, retry-friendly form."""
    parts: list[str] = []
    for error in errors:
        path = "/" + "/".join(str(segment) for segment in error.absolute_path)
        parts.append(f"{path or '/'}: {error.message}")
    return "; ".join(parts)


def _validate_schema_fragment(data: Any, schema_name: str, fragment_path: str) -> None:
    """Validate data against a schema fragment and raise field-level ValueError."""
    schema, registry = _load_schema_with_store(schema_name)
    wrapper_schema = {
        "$schema": schema.get("$schema", "https://json-schema.org/draft/2020-12/schema"),
        "$ref": f"{schema_name}#/{fragment_path}",
    }
    validator = Draft202012Validator(wrapper_schema, registry=registry)
    errors = sorted(validator.iter_errors(data), key=lambda err: list(err.absolute_path))
    if errors:
        raise ValueError(
            f"Schema validation failed for {schema_name}#/{fragment_path}: "
            f"{_format_validation_errors(errors)}"
        )


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
) -> dict[str, Any]:
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

    return {
        "table": fqn_norm,
        "object_type": object_type,
        "view_sql": view_sql,
        "profile": profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        "columns": columns,
        "source_tables": source_tables,
        "test_spec": test_spec,
        "sandbox": sandbox,
    }


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> dict[str, Any]:
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

    return {
        "table": fqn_norm,
        "writer": writer_norm,
        "proc_body": proc_body,
        "profile": profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        "statements": statements,
        "columns": columns,
        "source_tables": source_tables,
        "source_columns": source_columns,
        "test_spec": test_spec,
        "sandbox": sandbox,
        "writer_ddl_slice": writer_ddl_slice,
    }


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

    failed_scenarios = [
        result.get("scenario_name", "unknown")
        for result in compare_sql_result.get("results", [])
        if result.get("status") != "ok" or result.get("equivalent") is False
    ]
    scenarios_total = int(compare_sql_result.get("total", 0))
    scenarios_passed = int(compare_sql_result.get("passed", 0))
    return {
        "required": compare_required,
        "executed": True,
        "passed": len(failed_scenarios) == 0 and scenarios_total > 0,
        "scenarios_total": scenarios_total,
        "scenarios_passed": scenarios_passed,
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
) -> dict[str, Any]:
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
        _validate_schema_fragment(refactor_data, "view_catalog.json", "properties/refactor")
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
    _validate_schema_fragment(refactor_data, "table_catalog.json", "properties/refactor")

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
    return {
        "ok": True,
        "table": table_norm,
        "status": status,
        "writer": writer_norm,
        "catalog_path": str(catalog_path),
    }


def _run_write_view(
    project_root: Path,
    fqn_norm: str,
    refactor_data: dict[str, Any],
) -> dict[str, Any]:
    """Write refactor block to a view catalog file."""
    result = load_and_merge_catalog(project_root, fqn_norm, "refactor", refactor_data)
    result["object_type"] = "view"
    logger.info(
        "event=write_complete object_type=view view=%s catalog_path=%s",
        fqn_norm, result["catalog_path"],
    )
    return result


# ── Sweep ────────────────────────────────────────────────────────────────────


def _check_stg_models(
    dbt_path: Path, source_tables: list[str],
) -> list[str]:
    """Return names of existing stg_*.sql files for the given source tables."""
    staging_dir = dbt_path / "models" / "staging"
    found: list[str] = []
    for fqn in source_tables:
        _, name = fqn_parts(fqn)
        stg_file = staging_dir / f"stg_{name.lower()}.sql"
        if stg_file.exists():
            found.append(stg_file.name)
    return found


def _check_mart_model(dbt_path: Path, fqn: str) -> str | None:
    """Return relative path of mart model if it exists on disk, else None."""
    _, name = fqn_parts(fqn)
    model_name = name.lower()
    mart_file = dbt_path / "models" / "marts" / f"{model_name}.sql"
    if mart_file.exists():
        return str(mart_file.relative_to(dbt_path))
    return None


def _recommend_action(refactor_status: str | None) -> str:
    """Derive recommended_action from refactor.status."""
    if refactor_status == "ok":
        return "skip"
    if refactor_status == "partial":
        return "re-refactor"
    return "refactor"


def run_sweep(
    project_root: Path,
    fqns: list[str],
) -> dict[str, Any]:
    """Run the planning sweep across a batch of FQNs.

    For each FQN, reads catalog status and checks for existing dbt models.
    Detects shared staging candidates across the batch and persists
    ``refactor.shared_sources`` on each affected catalog entry.

    Returns the sweep plan artifact dict.
    """
    dbt_path = resolve_dbt_project_path(project_root)
    catalog_dir = resolve_catalog_dir(project_root)

    objects: list[dict[str, Any]] = []
    # Track source tables per non-skip FQN for shared staging detection
    source_map: dict[str, list[str]] = {}

    for raw_fqn in fqns:
        fqn_norm = normalize(raw_fqn)

        # Auto-detect object type
        view_cat = load_view_catalog(project_root, fqn_norm)
        if view_cat is not None:
            refactor_status = view_cat.refactor.status if view_cat.refactor else None
            source_tables = collect_view_source_tables(project_root, fqn_norm)
            obj = {
                "fqn": fqn_norm,
                "object_type": "view",
                "writer": None,
                "refactor_status": refactor_status,
                "source_tables": source_tables,
                "existing_stg_models": _check_stg_models(dbt_path, source_tables),
                "existing_mart_model": _check_mart_model(dbt_path, fqn_norm),
                "recommended_action": _recommend_action(refactor_status),
            }
        else:
            writer_fqn = read_selected_writer(project_root, fqn_norm)
            refactor_status: str | None = None
            source_tables: list[str] = []
            if writer_fqn:
                writer_norm = normalize(writer_fqn)
                proc_cat = load_proc_catalog(project_root, writer_norm)
                if proc_cat:
                    refactor_status = proc_cat.refactor.status if proc_cat.refactor else None
                    source_tables = collect_source_tables(project_root, writer_norm)
            obj = {
                "fqn": fqn_norm,
                "object_type": "table",
                "writer": normalize(writer_fqn) if writer_fqn else None,
                "refactor_status": refactor_status,
                "source_tables": source_tables,
                "existing_stg_models": _check_stg_models(dbt_path, source_tables),
                "existing_mart_model": _check_mart_model(dbt_path, fqn_norm),
                "recommended_action": _recommend_action(refactor_status),
            }

        objects.append(obj)

        # Track sources for non-skip objects
        if obj["recommended_action"] != "skip":
            source_map[fqn_norm] = obj["source_tables"]

    # Detect shared staging candidates
    all_sources: list[str] = []
    for sources in source_map.values():
        all_sources.extend(sources)
    source_counts = Counter(all_sources)
    shared_staging = sorted(fqn for fqn, count in source_counts.items() if count >= 2)

    # Persist shared_sources on each affected catalog entry
    if shared_staging:
        _persist_shared_sources(project_root, catalog_dir, objects, shared_staging)

    logger.info(
        "event=sweep_complete objects=%d shared_staging=%d",
        len(objects), len(shared_staging),
    )

    return {
        "epoch": int(_time.time()),
        "objects": objects,
        "shared_staging_candidates": shared_staging,
    }


def _persist_shared_sources(
    project_root: Path,
    catalog_dir: Path,
    objects: list[dict[str, Any]],
    shared_staging: list[str],
) -> None:
    """Write shared_sources into each affected procedure/view catalog's refactor section."""
    shared_set = set(shared_staging)

    for obj in objects:
        if obj["recommended_action"] == "skip":
            continue
        obj_shared = sorted(set(obj["source_tables"]) & shared_set)
        if not obj_shared:
            continue

        fqn = obj["fqn"]
        if obj["object_type"] == "view":
            cat_path = catalog_dir / "views" / f"{fqn}.json"
        else:
            writer = obj.get("writer")
            if not writer:
                continue
            cat_path = catalog_dir / "procedures" / f"{writer}.json"

        if not cat_path.exists():
            logger.warning(
                "event=shared_sources_skip reason=catalog_missing path=%s", cat_path,
            )
            continue

        try:
            cat = json.loads(cat_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning(
                "event=shared_sources_skip reason=parse_error path=%s error=%s",
                cat_path, exc,
            )
            continue

        refactor_section = cat.setdefault("refactor", {})
        refactor_section["shared_sources"] = obj_shared
        _write_catalog_json(cat_path, cat)

        logger.info(
            "event=shared_sources_written fqn=%s shared=%s", fqn, obj_shared,
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
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def sweep(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    tables: list[str] = typer.Option(..., "--tables", help="Fully-qualified object names to sweep"),
) -> None:
    """Run planning sweep across a batch of objects for the /refactor command."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_sweep(project_root, tables)
    except (ValueError, CatalogLoadError) as exc:
        logger.error("event=sweep_failed error=%s", exc)
        emit({"ok": False, "error": str(exc)})
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError) as exc:
        logger.error("event=sweep_failed error=%s", exc)
        emit({"ok": False, "error": str(exc)})
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
