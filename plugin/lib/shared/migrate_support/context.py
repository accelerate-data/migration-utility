"""Context assembly helpers for migrate."""

from __future__ import annotations

from pathlib import Path

from shared.catalog import has_catalog, load_proc_catalog, read_selected_writer
from shared.context_helpers import (
    collect_source_tables,
    load_object_columns,
    load_proc_body,
    load_proc_statements,
    load_table_columns,
    load_table_profile,
)
from shared.loader import CatalogFileMissingError, CatalogNotFoundError
from shared.name_resolver import normalize
from shared.output_models import MigrateContextOutput

from .derivation import derive_materialization, derive_schema_tests


def _classify_proc(project_root: Path, writer_fqn: str) -> bool:
    """Return True if the procedure needs LLM analysis."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        raise CatalogFileMissingError("procedure", writer_fqn)
    return cat.mode == "llm_required"


def _load_refactored_sql(project_root: Path, table_fqn: str) -> str | None:
    """Load refactored SQL from the selected writer procedure catalog."""
    writer_fqn = read_selected_writer(project_root, table_fqn)
    if not writer_fqn:
        return None
    cat = load_proc_catalog(project_root, normalize(writer_fqn))
    if cat is None or not cat.refactor:
        return None
    return cat.refactor.refactored_sql or None


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> MigrateContextOutput:
    """Assemble migration context for a single table/writer pair."""
    table_norm = normalize(table_fqn)
    if not writer_fqn:
        writer_fqn = read_selected_writer(project_root, table_norm)
        if not writer_fqn:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {table_norm}"
            )
    writer_norm = normalize(writer_fqn)

    if not has_catalog(project_root):
        raise CatalogNotFoundError(project_root)

    profile = load_table_profile(project_root, table_norm)
    statements = load_proc_statements(project_root, writer_norm)
    needs_llm = _classify_proc(project_root, writer_norm)
    proc_body = load_proc_body(project_root, writer_norm)
    columns = load_table_columns(project_root, table_norm)
    source_tables = collect_source_tables(project_root, writer_norm)
    source_columns = {
        fqn: load_object_columns(project_root, fqn) for fqn in source_tables
    }
    materialization = derive_materialization(profile)
    schema_tests = derive_schema_tests(profile)
    refactored_sql = _load_refactored_sql(project_root, table_norm)

    return MigrateContextOutput(
        table=table_norm,
        writer=writer_norm,
        needs_llm=needs_llm,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        materialization=materialization,
        statements=statements,
        proc_body=proc_body,
        columns=columns,
        source_tables=source_tables,
        source_columns=source_columns,
        schema_tests=schema_tests,
        refactored_sql=refactored_sql,
    )
