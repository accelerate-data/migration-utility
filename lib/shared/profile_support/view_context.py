from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_view_catalog
from shared.context_helpers import target_visible_columns
from shared.loader import CatalogFileMissingError
from shared.name_resolver import normalize
from shared.output_models.discover import SqlElement
from shared.output_models.profile import (
    EnrichedInScopeRef,
    EnrichedScopedRefList,
    OutOfScopeRef,
    ViewColumnDef,
    ViewProfileContext,
    ViewReferencedBy,
    ViewReferences,
)

logger = logging.getLogger(__name__)


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

    refs_bucket = view_cat.references
    references = ViewReferences(
        tables=_build_enriched_ref_list(getattr(refs_bucket, "tables", None) if refs_bucket else None, "table"),
        views=_build_enriched_ref_list(getattr(refs_bucket, "views", None) if refs_bucket else None, "view"),
        functions=_build_enriched_ref_list(getattr(refs_bucket, "functions", None) if refs_bucket else None, "function"),
    )

    refby_bucket = view_cat.referenced_by
    referenced_by = ViewReferencedBy(
        procedures=_build_enriched_ref_list(getattr(refby_bucket, "procedures", None) if refby_bucket else None, "procedure"),
        views=_build_enriched_ref_list(getattr(refby_bucket, "views", None) if refby_bucket else None, "view"),
        functions=_build_enriched_ref_list(getattr(refby_bucket, "functions", None) if refby_bucket else None, "function"),
    )

    raw_elements = view_cat.scoping.sql_elements
    sql_elements = [
        SqlElement.model_validate(e.model_dump() if hasattr(e, "model_dump") else e)
        for e in raw_elements
    ] if raw_elements else None

    columns = [
        ViewColumnDef.model_validate(c) for c in target_visible_columns(view_cat.columns)
    ] if view_cat.is_materialized_view else []

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
