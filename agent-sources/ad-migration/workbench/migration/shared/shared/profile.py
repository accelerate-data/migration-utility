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
from typing import Any

import typer

from shared.catalog import (
    load_proc_catalog,
    load_table_catalog,
)
from shared.loader import (
    CatalogNotFoundError,
    DdlParseError,
    load_ddl,
)
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


# ── Helpers ──────────────────────────────────────────────────────────────────


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


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
    ddl_path: Path, ddl_catalog: Any, writer_references: dict[str, Any],
) -> list[dict[str, Any]]:
    """Load catalog + DDL body for each procedure in the writer's in_scope refs."""
    related: list[dict[str, Any]] = []
    proc_refs = writer_references.get("procedures", {})
    for ref_proc in proc_refs.get("in_scope", []):
        ref_fqn = normalize(f"{ref_proc['schema']}.{ref_proc['name']}")
        ref_cat = load_proc_catalog(ddl_path, ref_fqn)
        ref_entry = ddl_catalog.get_procedure(ref_fqn)
        ref_body = ref_entry.raw_ddl if ref_entry else ""
        entry: dict[str, Any] = {"procedure": ref_fqn, "proc_body": ref_body}
        if ref_cat is not None:
            entry["references"] = ref_cat.get("references", {})
        related.append(entry)
    return related


def run_context(ddl_path: Path, table: str, writer: str) -> dict[str, Any]:
    """Assemble profiling context for a table + writer pair.

    Returns a dict matching ``schemas/profile_context.json``.
    """
    table_norm = normalize(table)
    writer_norm = normalize(writer)

    # Load table catalog
    table_cat = load_table_catalog(ddl_path, table_norm)
    if table_cat is None:
        logger.error("event=context_failed operation=load_table_catalog table=%s reason=no_catalog_file", table_norm)
        raise typer.Exit(code=1)

    catalog_signals = _extract_catalog_signals(table_cat)

    # Load writer procedure catalog
    proc_cat = load_proc_catalog(ddl_path, writer_norm)
    if proc_cat is None:
        logger.error("event=context_failed operation=load_proc_catalog table=%s writer=%s reason=no_catalog_file", table_norm, writer_norm)
        raise typer.Exit(code=1)

    writer_references = proc_cat.get("references", {})

    # Load proc body from DDL files
    ddl_catalog, _ = load_ddl(ddl_path)
    proc_entry = ddl_catalog.get_procedure(writer_norm)
    proc_body = proc_entry.raw_ddl if proc_entry else ""
    if not proc_body:
        logger.warning("event=context_warning operation=load_proc_body table=%s writer=%s reason=no_ddl_body", table_norm, writer_norm)

    related_procedures = _build_related_procedures(ddl_path, ddl_catalog, writer_references)

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


def run_write(ddl_path: Path, table: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a table catalog file.

    Returns a confirmation dict on success.
    Raises typer.Exit(1) on validation failure, typer.Exit(2) on IO error.
    """
    table_norm = normalize(table)

    # Validate profile
    errors = _validate_profile(profile_json)
    if errors:
        error_result = {"ok": False, "errors": errors}
        logger.error("event=write_failed operation=validate table=%s error_count=%d", table_norm, len(errors))
        _emit(error_result)
        raise typer.Exit(code=1)

    # Load existing catalog file
    catalog_path = ddl_path / "catalog" / "tables" / f"{table_norm}.json"
    if not catalog_path.exists():
        logger.error("event=write_failed operation=read_catalog table=%s reason=file_not_found", table_norm)
        raise typer.Exit(code=2)

    try:
        existing = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("event=write_failed operation=read_catalog table=%s error=%s", table_norm, exc)
        raise typer.Exit(code=2) from exc

    # Merge profile section
    existing["profile"] = profile_json

    # Atomic write (write to temp, then rename)
    tmp_path = catalog_path.with_suffix(".json.tmp")
    try:
        tmp_path.write_text(
            json.dumps(existing, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(catalog_path)
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        logger.error("event=write_failed operation=atomic_write table=%s error=%s", table_norm, exc)
        raise typer.Exit(code=2) from exc

    logger.info("event=write_complete table=%s catalog_path=%s", table_norm, catalog_path)
    return {
        "ok": True,
        "table": table_norm,
        "catalog_path": str(catalog_path),
    }


# ── CLI commands ─────────────────────────────────────────────────────────────


@app.command()
def context(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: str = typer.Option(..., help="Fully-qualified writer procedure name"),
) -> None:
    """Assemble profiling context for a table + writer pair."""
    try:
        result = run_context(ddl_path, table, writer)
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def write(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    profile: str = typer.Option(..., help="Profile JSON string"),
) -> None:
    """Validate and merge a profile section into a table catalog file."""
    try:
        profile_data = json.loads(profile)
    except json.JSONDecodeError as exc:
        logger.error("event=write_failed operation=parse_json table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc

    try:
        result = run_write(ddl_path, table, profile_data)
    except (FileNotFoundError, OSError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    _emit(result)


if __name__ == "__main__":
    app()
