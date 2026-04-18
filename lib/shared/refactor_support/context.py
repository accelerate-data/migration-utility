"""Context assembly helpers for refactor."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.catalog import load_proc_catalog, load_view_catalog, read_selected_writer
from shared.context_helpers import (
    collect_source_tables_from_sql,
    collect_source_tables,
    collect_view_source_tables,
    load_object_columns,
    load_proc_body,
    load_proc_statements,
    load_table_columns,
    load_table_profile,
    load_test_spec,
    project_sql_dialect,
    resolve_selected_writer_ddl_slice,
    sandbox_metadata,
    target_visible_columns,
)
from shared.loader import CatalogFileMissingError
from shared.name_resolver import normalize
from shared.output_models.refactor import RefactorContextOutput


def _run_context_view(project_root: Path, fqn_norm: str, cat: Any) -> RefactorContextOutput:
    """Assemble refactoring context for a view or materialized view."""
    view_sql = cat.sql
    if not view_sql:
        raise ValueError(f"View catalog for {fqn_norm} has no 'sql' key")
    profile = cat.profile
    if profile is None:
        raise ValueError(f"View catalog for {fqn_norm} has no 'profile' section — run /profile first")

    return RefactorContextOutput(
        table=fqn_norm,
        object_type="mv" if cat.is_materialized_view else "view",
        view_sql=view_sql,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        columns=target_visible_columns(cat.columns),
        source_tables=collect_view_source_tables(project_root, fqn_norm),
        test_spec=load_test_spec(project_root, fqn_norm),
        sandbox=sandbox_metadata(project_root),
    )


def run_context(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str | None = None,
) -> RefactorContextOutput:
    """Assemble refactoring context for a table, view, or materialized view."""
    fqn_norm = normalize(table_fqn)
    if not writer_fqn:
        view_cat = load_view_catalog(project_root, fqn_norm)
        if view_cat is not None:
            return _run_context_view(project_root, fqn_norm, view_cat)

    if not writer_fqn:
        writer_fqn = read_selected_writer(project_root, fqn_norm)
        if not writer_fqn:
            raise ValueError(
                f"No writer provided and no scoping.selected_writer in catalog for {fqn_norm}"
            )
    writer_norm = normalize(writer_fqn)

    proc_cat = load_proc_catalog(project_root, writer_norm)
    if proc_cat is None:
        raise CatalogFileMissingError("procedure", writer_norm)
    selected_writer_ddl_slice = resolve_selected_writer_ddl_slice(proc_cat, fqn_norm, writer_norm)

    profile = load_table_profile(project_root, fqn_norm)
    statements = [] if selected_writer_ddl_slice else load_proc_statements(project_root, writer_norm)
    proc_body = "" if selected_writer_ddl_slice else load_proc_body(project_root, writer_norm)
    columns = load_table_columns(project_root, fqn_norm)
    dialect = project_sql_dialect(project_root)
    source_tables = (
        collect_source_tables_from_sql(selected_writer_ddl_slice, dialect=dialect)
        if selected_writer_ddl_slice
        else collect_source_tables(project_root, writer_norm)
    )
    source_columns = {
        fqn: load_object_columns(project_root, fqn) for fqn in source_tables
    }

    return RefactorContextOutput(
        table=fqn_norm,
        writer=writer_norm,
        proc_body=proc_body,
        profile=profile.model_dump(by_alias=True, exclude_none=True) if hasattr(profile, "model_dump") else profile,
        statements=statements,
        columns=columns,
        source_tables=source_tables,
        source_columns=source_columns,
        test_spec=load_test_spec(project_root, fqn_norm),
        sandbox=sandbox_metadata(project_root),
        selected_writer_ddl_slice=selected_writer_ddl_slice,
    )
