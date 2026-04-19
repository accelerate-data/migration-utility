from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_proc_catalog, load_table_catalog, read_selected_writer
from shared.catalog_models import ReferencesBucket, TableCatalog
from shared.context_helpers import (
    project_sql_dialect,
    references_from_selected_sql,
    resolve_selected_writer_ddl_slice,
    target_visible_columns,
)
from shared.loader import CatalogFileMissingError, load_ddl
from shared.name_resolver import normalize
from shared.output_models.profile import (
    CatalogSignals,
    ProfileColumnDef,
    ProfileContext,
    RelatedProcedure,
)

logger = logging.getLogger(__name__)


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

    columns = [ProfileColumnDef.model_validate(c) for c in target_visible_columns(table_cat.columns)]

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
