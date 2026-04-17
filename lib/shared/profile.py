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
from pydantic import ValidationError

from shared.catalog import (
    load_and_merge_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
    write_json as _write_catalog_json,
)
from shared.catalog_models import ReferencesBucket, TableCatalog, TableProfileSection, ViewProfileSection
from shared.context_helpers import (
    project_sql_dialect,
    references_from_selected_sql,
    resolve_selected_writer_ddl_slice,
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
from shared.output_models.discover import SqlElement
from shared.output_models.profile import (
    CatalogSignals,
    EnrichedInScopeRef,
    EnrichedScopedRefList,
    OutOfScopeRef,
    ProfileColumnDef,
    ProfileContext,
    RelatedProcedure,
    ViewColumnDef,
    ViewProfileContext,
    ViewReferencedBy,
    ViewReferences,
)

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def build_seed_profile(rationale: str = "Table is maintained as a dbt seed.") -> dict[str, Any]:
    """Build the canonical profile payload for a dbt seed table."""
    return {
        "classification": {
            "resolved_kind": "seed",
            "source": "catalog",
            "rationale": rationale,
        },
        "warnings": [],
        "errors": [],
    }



# ── Context assembly (importable for testing) ────────────────────────────────


def _extract_catalog_signals(table_cat: TableCatalog) -> CatalogSignals:
    """Pull the six catalog signal categories from a table catalog dict."""
    return CatalogSignals.model_validate({
        "primary_keys": table_cat.primary_keys,
        "foreign_keys": table_cat.foreign_keys,
        "auto_increment_columns": table_cat.auto_increment_columns,
        "unique_indexes": table_cat.unique_indexes,
        "change_capture": table_cat.change_capture,
        "sensitivity_classifications": table_cat.sensitivity_classifications,
    })


def _build_related_procedures(
    project_root: Path, ddl_catalog: Any, writer_references: Any,
) -> list[RelatedProcedure]:
    """Load catalog + DDL body for each procedure in the writer's in_scope refs."""
    related: list[RelatedProcedure] = []
    if writer_references is None:
        return related
    proc_refs = writer_references.procedures
    for ref_proc in proc_refs.in_scope:
        ref_fqn = normalize(f"{ref_proc.object_schema}.{ref_proc.name}")
        ref_cat = load_proc_catalog(project_root, ref_fqn)
        ref_entry = ddl_catalog.get_procedure(ref_fqn)
        ref_body = ref_entry.raw_ddl if ref_entry else ""
        refs = None
        if ref_cat is not None:
            refs = ref_cat.references.model_dump(by_alias=True, exclude_none=True) if ref_cat.references else {}
        related.append(RelatedProcedure(procedure=ref_fqn, proc_body=ref_body, references=refs))
    return related


def run_context(project_root: Path, table: str, writer: str | None = None) -> ProfileContext:
    """Assemble profiling context for a table + writer pair.

    If *writer* is not provided, reads ``scoping.selected_writer`` from
    the table catalog.  Raises ``ValueError`` if neither is available.

    Returns a ``ProfileContext`` model instance.
    """
    table_norm = normalize(table)
    table_cat = load_table_catalog(project_root, table_norm)
    if table_cat is None:
        raise CatalogFileMissingError("table", table_norm)
    if table_cat.is_seed:
        raise ValueError(f"Cannot build writer-driven profiling context for seed table {table_norm}")

    if not writer:
        writer = read_selected_writer(project_root, table_norm)
        if not writer:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {table_norm}"
            )
    writer_norm = normalize(writer)

    catalog_signals = _extract_catalog_signals(table_cat)

    # Load writer procedure catalog
    proc_cat = load_proc_catalog(project_root, writer_norm)
    if proc_cat is None:
        raise CatalogFileMissingError("procedure", writer_norm)

    selected_writer_ddl_slice = resolve_selected_writer_ddl_slice(proc_cat, table_norm, writer_norm)

    if selected_writer_ddl_slice:
        writer_references = references_from_selected_sql(
            selected_writer_ddl_slice,
            dialect=project_sql_dialect(project_root),
        )
        proc_body = ""
        related_procedures = []
    else:
        writer_references = proc_cat.references or ReferencesBucket()
        ddl_catalog, _ = load_ddl(project_root)
        proc_entry = ddl_catalog.get_procedure(writer_norm)
        proc_body = proc_entry.raw_ddl if proc_entry else ""
        if not proc_body:
            logger.warning("event=context_warning operation=load_proc_body table=%s writer=%s reason=no_ddl_body", table_norm, writer_norm)
        related_procedures = _build_related_procedures(project_root, ddl_catalog, writer_references)

    columns = [ProfileColumnDef.model_validate(c) for c in table_cat.columns]

    logger.info("event=context_assembled table=%s writer=%s related_count=%d", table_norm, writer_norm, len(related_procedures))
    return ProfileContext(
        table=table_norm,
        writer=writer_norm,
        catalog_signals=catalog_signals,
        writer_references=writer_references,
        proc_body=proc_body,
        columns=columns,
        related_procedures=related_procedures,
        selected_writer_ddl_slice=selected_writer_ddl_slice,
    )


def _build_enriched_ref_list(
    scoped: Any | None, obj_type: str,
) -> EnrichedScopedRefList:
    """Build an enriched scoped ref list with object_type on each in_scope entry."""
    if scoped is None:
        return EnrichedScopedRefList()
    in_scope = [
        EnrichedInScopeRef(
            **{**e.model_dump(by_alias=True, exclude_none=True), "object_type": obj_type},
        )
        for e in scoped.in_scope
    ]
    out_of_scope = [
        OutOfScopeRef(schema=e.object_schema, name=e.name)
        for e in scoped.out_of_scope
    ]
    return EnrichedScopedRefList(in_scope=in_scope, out_of_scope=out_of_scope)


def run_view_context(project_root: Path, view_fqn: str) -> ViewProfileContext:
    """Assemble view profiling context from view catalog.

    Adds object_type to each in_scope entry across references and referenced_by.
    Returns a ``ViewProfileContext`` model instance.
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
    references = ViewReferences(
        tables=_build_enriched_ref_list(getattr(refs_bucket, "tables", None) if refs_bucket else None, "table"),
        views=_build_enriched_ref_list(getattr(refs_bucket, "views", None) if refs_bucket else None, "view"),
        functions=_build_enriched_ref_list(getattr(refs_bucket, "functions", None) if refs_bucket else None, "function"),
    )

    # Enrich referenced_by: add object_type to each in_scope entry
    refby_bucket = view_cat.referenced_by
    referenced_by = ViewReferencedBy(
        procedures=_build_enriched_ref_list(getattr(refby_bucket, "procedures", None) if refby_bucket else None, "procedure"),
        views=_build_enriched_ref_list(getattr(refby_bucket, "views", None) if refby_bucket else None, "view"),
        functions=_build_enriched_ref_list(getattr(refby_bucket, "functions", None) if refby_bucket else None, "function"),
    )

    # Build sql_elements as typed models — catalog loads as catalog_models.SqlElement,
    # convert to output_models.SqlElement via dict round-trip
    raw_elements = view_cat.scoping.sql_elements
    sql_elements = [
        SqlElement.model_validate(e.model_dump() if hasattr(e, "model_dump") else e)
        for e in raw_elements
    ] if raw_elements else None

    columns = [ViewColumnDef.model_validate(c) for c in view_cat.columns] if view_cat.is_materialized_view else []

    logger.info("event=view_context_assembled view=%s", view_norm)
    return ViewProfileContext(
        view=view_norm,
        is_materialized_view=view_cat.is_materialized_view,
        sql_elements=sql_elements,
        logic_summary=view_cat.scoping.logic_summary,
        columns=columns,
        references=references,
        referenced_by=referenced_by,
        warnings=getattr(view_cat, "warnings", []),
        errors=getattr(view_cat, "errors", []),
    )


# ── Write validation and merge (importable for testing) ──────────────────────


def derive_table_profile_status(section: TableProfileSection) -> str:
    """Derive the persisted status for a validated table profile."""
    resolved_kind = section.classification.resolved_kind if section.classification else None
    if resolved_kind == "seed":
        return "ok"
    if resolved_kind and section.primary_key is not None:
        return "ok"
    if resolved_kind:
        return "partial"
    return "error"


def derive_view_profile_status(section: ViewProfileSection) -> str:
    """Derive the persisted status for a validated view profile."""
    return "ok"


def _profile_payload_with_status(
    section: TableProfileSection | ViewProfileSection,
    status: str,
) -> dict[str, Any]:
    """Return the validated profile payload with derived status preserved."""
    return section.model_copy(update={"status": status}).model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude_unset=True,
    )


def _write_view_profile(project_root: Path, view_norm: str, profile_json: dict[str, Any]) -> dict[str, Any]:
    """Validate and merge a profile section into a view catalog file."""
    profile_section = ViewProfileSection.model_validate(profile_json)
    profile_payload = _profile_payload_with_status(
        profile_section,
        derive_view_profile_status(profile_section),
    )

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

    existing["profile"] = profile_payload

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

    existing_table = load_table_catalog(project_root, norm)
    if existing_table is None:
        raise CatalogFileMissingError("table", norm)

    profile_section = TableProfileSection.model_validate(profile_json)
    resolved_kind = (
        profile_section.classification.resolved_kind
        if profile_section.classification is not None
        else None
    )
    if existing_table.is_seed and resolved_kind != "seed":
        raise ValueError(f"seed table profiles must use seed classification for {norm}")
    if resolved_kind == "seed" and not existing_table.is_seed:
        raise ValueError(f"seed classification requires is_seed: true for {norm}")

    profile_payload = _profile_payload_with_status(
        profile_section,
        derive_table_profile_status(profile_section),
    )

    result = load_and_merge_catalog(project_root, norm, "profile", profile_payload)
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
    except (ValueError, ValidationError, CatalogFileMissingError) as exc:
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
