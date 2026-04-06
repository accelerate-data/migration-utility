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

from shared.catalog import (
    load_proc_catalog,
    load_table_catalog,
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


# ── Context assembly (importable for testing) ────────────────────────────────


def _extract_catalog_signals(table_cat: dict[str, Any]) -> dict[str, Any]:
    """Pull the six catalog signal categories from a table catalog dict."""
    return {
        "primary_keys": table_cat.get("primary_keys", []),
        "foreign_keys": table_cat.get("foreign_keys", []),
        "auto_increment_columns": table_cat.get("auto_increment_columns", []),
        "unique_indexes": table_cat.get("unique_indexes", []),
        "change_capture": table_cat.get("change_capture"),
        "sensitivity_classifications": table_cat.get("sensitivity_classifications", []),
    }


def _build_related_procedures(
    project_root: Path, ddl_catalog: Any, writer_references: dict[str, Any],
) -> list[dict[str, Any]]:
    """Load catalog + DDL body for each procedure in the writer's in_scope refs."""
    related: list[dict[str, Any]] = []
    proc_refs = writer_references.get("procedures", {})
    for ref_proc in proc_refs.get("in_scope", []):
        ref_fqn = normalize(f"{ref_proc['schema']}.{ref_proc['name']}")
        ref_cat = load_proc_catalog(project_root, ref_fqn)
        ref_entry = ddl_catalog.get_procedure(ref_fqn)
        ref_body = ref_entry.raw_ddl if ref_entry else ""
        entry: dict[str, Any] = {"procedure": ref_fqn, "proc_body": ref_body}
        if ref_cat is not None:
            entry["references"] = ref_cat.get("references", {})
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

    writer_references = proc_cat.get("references", {})

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
        "writer_references": writer_references,
        "proc_body": proc_body,
        "columns": table_cat.get("columns", []),
        "related_procedures": related_procedures,
    }


# ── Write validation and merge (importable for testing) ──────────────────────


def _validate_profile(profile: dict[str, Any]) -> list[str]:
    """Validate a profile dict. Returns a list of error messages (empty = valid)."""
    errors: list[str] = []

    # Required top-level fields
    for field in ("status", "writer"):
        if field not in profile:
            errors.append(f"missing required field: {field}")

    # Status enum
    status = profile.get("status")
    if status is not None and status not in PROFILE_STATUSES:
        errors.append(f"invalid status: {status!r}, must be one of {sorted(PROFILE_STATUSES)}")

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


def run_write(project_root: Path, table: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a table catalog file.

    Returns a confirmation dict on success.
    Raises ValueError on validation failure, OSError/json.JSONDecodeError on IO error.
    """
    table_norm = normalize(table)

    # Validate profile
    errors = _validate_profile(profile_json)
    if errors:
        raise ValueError(f"Profile validation failed for {table_norm}: {'; '.join(errors)}")

    # Load existing catalog file
    catalog_path = resolve_catalog_dir(project_root) / "tables" / f"{table_norm}.json"
    if not catalog_path.exists():
        raise CatalogFileMissingError("table", table_norm)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    except OSError as exc:
        logger.error("event=write_failed operation=read_catalog table=%s error=%s", table_norm, exc)
        raise

    # Merge profile section
    existing["profile"] = profile_json

    try:
        _write_catalog_json(catalog_path, existing)
    except OSError as exc:
        logger.error("event=write_failed operation=atomic_write table=%s error=%s", table_norm, exc)
        raise

    logger.info("event=write_complete table=%s catalog_path=%s", table_norm, catalog_path)
    return {
        "ok": True,
        "table": table_norm,
        "catalog_path": str(catalog_path),
    }


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
