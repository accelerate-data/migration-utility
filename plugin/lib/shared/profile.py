"""profile.py -- Profiling context assembly and catalog write-back.

Standalone CLI with two subcommands:

    context  Assemble all deterministic context needed for LLM profiling.
    write    Validate and merge a profile section into a table catalog file.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain/validation failure
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
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
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    load_ddl,
)
from shared.cli_utils import emit
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)
_SCHEMA_DIR = Path(__file__).with_name("schemas")


# ── Constants ────────────────────────────────────────────────────────────────

RESOLVED_KINDS = frozenset({
    "dim_non_scd",
    "dim_scd1",
    "dim_scd2",
    "dim_junk",
    "fact_transaction",
    "fact_periodic_snapshot",
    "fact_accumulating_snapshot",
    "fact_aggregate",
})

FK_TYPES = frozenset({"standard", "role_playing", "degenerate"})

SUGGESTED_ACTIONS = frozenset({"mask", "drop", "tokenize", "keep"})

SOURCES = frozenset({"catalog", "llm", "catalog+llm"})

PROFILE_STATUSES = frozenset({"ok", "partial", "error"})

PK_TYPES = frozenset({"surrogate", "natural", "composite", "unknown"})

VIEW_CLASSIFICATIONS = frozenset({"stg", "mart"})
VIEW_SOURCES = frozenset({"llm"})


def _load_schema_with_store(schema_name: str) -> tuple[dict[str, Any], Registry]:
    """Load a schema file and registry for local schema references."""
    registry = Registry()
    loaded: dict[str, Any] = {}
    for schema_path in _SCHEMA_DIR.glob("*.json"):
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        loaded[schema_path.name] = schema
        resource = Resource.from_contents(schema)
        registry = registry.with_resource(schema_path.name, resource)
        registry = registry.with_resource(schema_path.resolve().as_uri(), resource)
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


# ── Context assembly (importable for testing) ────────────────────────────────


def _extract_catalog_signals(table_cat: dict[str, Any]) -> dict[str, Any]:
    """Pull the six catalog signal categories from a table catalog dict."""
    return {
        "primary_keys": table_cat.primary_keys,
        "foreign_keys": table_cat.foreign_keys,
        "auto_increment_columns": table_cat.auto_increment_columns,
        "unique_indexes": table_cat.unique_indexes,
        "change_capture": table_cat.change_capture,
        "sensitivity_classifications": table_cat.sensitivity_classifications,
    }


def _build_related_procedures(
    project_root: Path, ddl_catalog: Any, writer_references: Any,
) -> list[dict[str, Any]]:
    """Load catalog + DDL body for each procedure in the writer's in_scope refs."""
    related: list[dict[str, Any]] = []
    if writer_references is None:
        return related
    proc_refs = writer_references.procedures
    for ref_proc in proc_refs.in_scope:
        ref_fqn = normalize(f"{ref_proc.object_schema}.{ref_proc.name}")
        ref_cat = load_proc_catalog(project_root, ref_fqn)
        ref_entry = ddl_catalog.get_procedure(ref_fqn)
        ref_body = ref_entry.raw_ddl if ref_entry else ""
        entry: dict[str, Any] = {"procedure": ref_fqn, "proc_body": ref_body}
        if ref_cat is not None:
            entry["references"] = ref_cat.references.model_dump(by_alias=True, exclude_none=True) if ref_cat.references else {}
        related.append(entry)
    return related


def run_context(project_root: Path, table: str, writer: str | None = None) -> dict[str, Any]:
    """Assemble profiling context for a table + writer pair.

    If *writer* is not provided, reads ``scoping.selected_writer`` from
    the table catalog.  Raises ``ValueError`` if neither is available.

    Returns a dict matching ``schemas/profile_context.json``.
    """
    table_norm = normalize(table)
    if not writer:
        writer = read_selected_writer(project_root, table_norm)
        if not writer:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {table_norm}"
            )
    writer_norm = normalize(writer)

    # Load table catalog
    table_cat = load_table_catalog(project_root, table_norm)
    if table_cat is None:
        raise CatalogFileMissingError("table", table_norm)

    catalog_signals = _extract_catalog_signals(table_cat)

    # Load writer procedure catalog
    proc_cat = load_proc_catalog(project_root, writer_norm)
    if proc_cat is None:
        raise CatalogFileMissingError("procedure", writer_norm)

    table_slices = proc_cat.table_slices or {}
    writer_ddl_slice = table_slices.get(table_norm) or None

    writer_references = proc_cat.references

    # Load proc body from DDL files
    ddl_catalog, _ = load_ddl(project_root)
    proc_entry = ddl_catalog.get_procedure(writer_norm)
    proc_body = proc_entry.raw_ddl if proc_entry else ""
    if not proc_body:
        logger.warning("event=context_warning operation=load_proc_body table=%s writer=%s reason=no_ddl_body", table_norm, writer_norm)

    related_procedures = _build_related_procedures(project_root, ddl_catalog, writer_references)

    logger.info("event=context_assembled table=%s writer=%s related_count=%d", table_norm, writer_norm, len(related_procedures))
    return {
        "table": table_norm,
        "writer": writer_norm,
        "catalog_signals": catalog_signals,
        "writer_references": writer_references.model_dump(by_alias=True, exclude_none=True) if writer_references else {},
        "proc_body": proc_body,
        "columns": table_cat.columns,
        "related_procedures": related_procedures,
        "writer_ddl_slice": writer_ddl_slice,
    }


def run_view_context(project_root: Path, view_fqn: str) -> dict[str, Any]:
    """Assemble view profiling context from view catalog.

    Adds object_type to each in_scope entry across references and referenced_by.
    Returns a dict matching schemas/view_profile_context.json.
    """
    view_norm = normalize(view_fqn)

    view_cat = load_view_catalog(project_root, view_norm)
    if view_cat is None:
        raise CatalogFileMissingError("view", view_norm)

    if view_cat.scoping is None or view_cat.scoping.status != "analyzed":
        raise ValueError(
            f"View scoping not completed for {view_norm}. Run analyzing-view first."
        )

    # Enrich references: add object_type to each in_scope entry
    refs_bucket = view_cat.references
    references: dict[str, Any] = {}
    for bucket, obj_type in (("tables", "table"), ("views", "view"), ("functions", "function")):
        scoped = getattr(refs_bucket, bucket, None) if refs_bucket else None
        in_scope = [e.model_dump(by_alias=True, exclude_none=True) for e in scoped.in_scope] if scoped else []
        out_of_scope = [e.model_dump(by_alias=True, exclude_none=True) for e in scoped.out_of_scope] if scoped else []
        references[bucket] = {
            "in_scope": [{**e, "object_type": obj_type} for e in in_scope],
            "out_of_scope": out_of_scope,
        }

    # Enrich referenced_by: add object_type to each in_scope entry
    refby_bucket = view_cat.referenced_by
    referenced_by: dict[str, Any] = {}
    for bucket, obj_type in (("procedures", "procedure"), ("views", "view"), ("functions", "function")):
        scoped = getattr(refby_bucket, bucket, None) if refby_bucket else None
        in_scope = [e.model_dump(by_alias=True, exclude_none=True) for e in scoped.in_scope] if scoped else []
        out_of_scope = [e.model_dump(by_alias=True, exclude_none=True) for e in scoped.out_of_scope] if scoped else []
        referenced_by[bucket] = {
            "in_scope": [{**e, "object_type": obj_type} for e in in_scope],
            "out_of_scope": out_of_scope,
        }

    result: dict[str, Any] = {
        "view": view_norm,
        "is_materialized_view": view_cat.is_materialized_view,
        "sql_elements": view_cat.scoping.sql_elements,
        "logic_summary": view_cat.scoping.logic_summary,
        "references": references,
        "referenced_by": referenced_by,
        "warnings": getattr(view_cat, "warnings", []),
        "errors": getattr(view_cat, "errors", []),
    }
    if view_cat.is_materialized_view:
        result["columns"] = view_cat.columns

    logger.info("event=view_context_assembled view=%s", view_norm)
    return result


# ── Write validation and merge (importable for testing) ──────────────────────


def _validate_profile(profile: dict[str, Any]) -> list[str]:
    """Validate a profile dict. Returns a list of error messages (empty = valid)."""
    errors: list[str] = []

    # Required top-level fields
    for field in ("writer",):
        if field not in profile:
            errors.append(f"missing required field: {field}")

    # Classification validation
    classification = profile.get("classification")
    if classification is not None:
        rk = classification.get("resolved_kind")
        if rk is not None and rk not in RESOLVED_KINDS:
            errors.append(f"invalid classification.resolved_kind: {rk!r}, must be one of {sorted(RESOLVED_KINDS)}")
        src = classification.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid classification.source: {src!r}, must be one of {sorted(SOURCES)}")

    # Primary key validation
    pk = profile.get("primary_key")
    if pk is not None:
        pk_type = pk.get("primary_key_type")
        if pk_type is not None and pk_type not in PK_TYPES:
            errors.append(f"invalid primary_key.primary_key_type: {pk_type!r}, must be one of {sorted(PK_TYPES)}")
        src = pk.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid primary_key.source: {src!r}, must be one of {sorted(SOURCES)}")

    # Natural key validation
    nk = profile.get("natural_key")
    if nk is not None:
        src = nk.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid natural_key.source: {src!r}, must be one of {sorted(SOURCES)}")

    # Watermark validation
    wm = profile.get("watermark")
    if wm is not None:
        src = wm.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid watermark.source: {src!r}, must be one of {sorted(SOURCES)}")

    # Foreign keys validation
    fks = profile.get("foreign_keys", [])
    for i, fk in enumerate(fks):
        fk_type = fk.get("fk_type")
        if fk_type is not None and fk_type not in FK_TYPES:
            errors.append(f"invalid foreign_keys[{i}].fk_type: {fk_type!r}, must be one of {sorted(FK_TYPES)}")
        src = fk.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid foreign_keys[{i}].source: {src!r}, must be one of {sorted(SOURCES)}")

    # PII actions validation
    pii = profile.get("pii_actions", [])
    for i, action in enumerate(pii):
        sa = action.get("suggested_action")
        if sa is not None and sa not in SUGGESTED_ACTIONS:
            errors.append(f"invalid pii_actions[{i}].suggested_action: {sa!r}, must be one of {sorted(SUGGESTED_ACTIONS)}")
        src = action.get("source")
        if src is not None and src not in SOURCES:
            errors.append(f"invalid pii_actions[{i}].source: {src!r}, must be one of {sorted(SOURCES)}")

    return errors


def _validate_view_profile(profile: dict[str, Any]) -> list[str]:
    """Validate a view profile dict. Returns a list of error messages (empty = valid)."""
    errors: list[str] = []

    for field in ("classification", "rationale", "source"):
        if field not in profile:
            errors.append(f"missing required field: {field}")

    classification = profile.get("classification")
    if classification is not None and classification not in VIEW_CLASSIFICATIONS:
        errors.append(f"invalid classification: {classification!r}, must be one of {sorted(VIEW_CLASSIFICATIONS)}")

    source = profile.get("source")
    if source is not None and source not in VIEW_SOURCES:
        errors.append(f"invalid source: {source!r}, must be one of {sorted(VIEW_SOURCES)}")

    return errors


def _write_view_profile(project_root: Path, view_norm: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a view catalog file."""
    errors = _validate_view_profile(profile_json)
    if errors:
        raise ValueError(f"View profile validation failed for {view_norm}: {'; '.join(errors)}")

    # Determine status from content
    classification = profile_json.get("classification")
    if classification is not None and classification in VIEW_CLASSIFICATIONS:
        status = "ok"
    else:
        status = "partial"
    profile_json["status"] = status
    _validate_schema_fragment(profile_json, "view_catalog.json", "properties/profile")

    catalog_path = resolve_catalog_dir(project_root) / "views" / f"{view_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("view", view_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    except OSError as exc:
        logger.error("event=write_failed operation=read_catalog view=%s error=%s", view_norm, exc)
        raise

    existing["profile"] = profile_json

    try:
        _write_catalog_json(catalog_path, existing)
    except OSError as exc:
        logger.error("event=write_failed operation=atomic_write view=%s error=%s", view_norm, exc)
        raise

    logger.info("event=write_complete view=%s catalog_path=%s", view_norm, catalog_path)
    return {
        "ok": True,
        "table": view_norm,
        "catalog_path": str(catalog_path),
    }


def run_write(project_root: Path, table: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a table or view catalog file.

    Auto-detects whether the FQN refers to a view (catalog/views/) or table
    (catalog/tables/). View path is checked first.

    Returns a confirmation dict on success.
    Raises ValueError on validation failure, OSError/json.JSONDecodeError on IO error.
    """
    if "status" in profile_json:
        raise ValueError("status must not be passed — determined by CLI")

    norm = normalize(table)

    # Auto-detect: check view catalog first
    view_catalog_path = resolve_catalog_dir(project_root) / "views" / f"{norm}.json"
    if view_catalog_path.exists():
        return _write_view_profile(project_root, norm, profile_json)

    # Validate table profile
    errors = _validate_profile(profile_json)
    if errors:
        raise ValueError(f"Profile validation failed for {norm}: {'; '.join(errors)}")

    # Determine status from content
    classification = profile_json.get("classification")
    has_classification = classification is not None and classification.get("resolved_kind") in RESOLVED_KINDS
    has_primary_key = profile_json.get("primary_key") is not None
    if has_classification and has_primary_key:
        status = "ok"
    elif has_classification:
        status = "partial"
    else:
        status = "error"
    profile_json["status"] = status
    _validate_schema_fragment(profile_json, "table_catalog.json", "$defs/profile_section")

    result = load_and_merge_catalog(project_root, norm, "profile", profile_json)
    logger.info("event=write_complete table=%s catalog_path=%s", norm, result["catalog_path"])
    return result


# ── CLI commands ─────────────────────────────────────────────────────────────


@app.command()
def context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
) -> None:
    """Assemble profiling context for a table + writer pair."""
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


@app.command(name="view-context")
def view_context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    view: str = typer.Option(..., "--view", help="Fully-qualified view name (schema.Name)"),
) -> None:
    """Assemble view profiling context for LLM classification."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_view_context(project_root, view)
    except CatalogFileMissingError as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, CatalogLoadError) as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    profile: str = typer.Option("", help="Profile JSON string"),
    profile_file: Optional[Path] = typer.Option(None, "--profile-file", help="Path to file containing profile JSON"),
) -> None:
    """Validate and merge a profile section into a table catalog file."""
    if profile_file:
        profile = profile_file.read_text(encoding="utf-8")
    if not profile:
        logger.error("event=write_failed table=%s error=no profile provided (use --profile or --profile-file)", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        profile_data = json.loads(profile)
    except json.JSONDecodeError as exc:
        logger.error("event=write_failed operation=parse_json table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc

    try:
        result = run_write(project_root, table, profile_data)
    except (ValueError, CatalogFileMissingError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
