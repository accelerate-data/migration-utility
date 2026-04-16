"""Browsable discover helpers split out of discover.py."""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn

from shared.catalog import (
    has_catalog,
    load_function_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    ObjectNotFoundError,
    extract_refs,
    load_ddl,
)
from shared.generate_sources import list_confirmed_source_tables
from shared.env_config import resolve_catalog_dir
from shared.name_resolver import normalize
from shared.output_models.discover import (
    BasicRefs,
    DiscoverListOutput,
    DiscoverRefsOutput,
    DiscoverShowOutput,
    ProcRefs,
    WriterEntry,
)
from shared.view_analysis import _analyze_view_select

logger = logging.getLogger(__name__)


class ObjectType(str, Enum):
    tables = "tables"
    sources = "sources"
    seeds = "seeds"
    procedures = "procedures"
    views = "views"
    functions = "functions"


def _load(project_root: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a DDL directory."""
    return load_ddl(project_root)


def _bucket(catalog: DdlCatalog, object_type: ObjectType) -> dict[str, DdlEntry]:
    return getattr(catalog, object_type.value)


def _find_entry(catalog: DdlCatalog, name: str) -> tuple[str, str, DdlEntry] | None:
    """Find an entry by normalized name across all buckets."""
    norm = normalize(name)
    for type_label, bucket_name in [
        ("table", "tables"),
        ("procedure", "procedures"),
        ("view", "views"),
        ("function", "functions"),
    ]:
        bucket: dict[str, DdlEntry] = getattr(catalog, bucket_name)
        if norm in bucket:
            return norm, type_label, bucket[norm]
    return None


def _catalog_error(type_label: str, norm: str) -> NoReturn:
    raise CatalogFileMissingError(type_label, norm)


def run_list(project_root: Path, object_type: ObjectType) -> DiscoverListOutput:
    """Return the list subcommand result."""
    if object_type == ObjectType.sources:
        return DiscoverListOutput(objects=list_confirmed_source_tables(project_root))
    if object_type == ObjectType.seeds:
        return DiscoverListOutput(objects=list_confirmed_seed_tables(project_root))
    catalog, _ = _load(project_root)
    return DiscoverListOutput(objects=sorted(_bucket(catalog, object_type).keys()))


def list_confirmed_seed_tables(project_root: Path) -> list[str]:
    """Return non-excluded catalog tables explicitly marked as dbt seeds."""
    tables_dir = resolve_catalog_dir(project_root) / "tables"
    if not tables_dir.is_dir():
        return []
    seeds: list[str] = []
    for path in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                path,
            )
            continue
        if cat.get("excluded") or cat.get("is_seed") is not True:
            continue
        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        seeds.append(f"{schema}.{name.lower()}")
    return seeds


def _show_table(project_root: Path, norm: str) -> dict[str, Any]:
    table_cat = load_table_catalog(project_root, norm)
    if table_cat is None:
        _catalog_error("table", norm)
    result: dict[str, Any] = {"columns": table_cat.columns}
    if table_cat.is_source:
        result["is_source"] = True
    if table_cat.is_seed:
        result["is_seed"] = True
    return result


def _show_procedure(project_root: Path, norm: str, entry: DdlEntry) -> dict[str, Any]:
    proc_cat = load_proc_catalog(project_root, norm)
    if proc_cat is None:
        _catalog_error("procedure", norm)

    refs_bucket = proc_cat.references
    tables_in_scope = refs_bucket.tables.in_scope if refs_bucket else []
    reads = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_selected]
    writes = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_updated]
    write_ops: dict[str, list[str]] = {}
    for table in tables_in_scope:
        if table.is_updated:
            tfqn = normalize(f"{table.object_schema}.{table.name}")
            ops = ["WRITE"]
            if table.is_insert_all:
                ops.append("INSERT")
            write_ops[tfqn] = ops
    funcs_in_scope = refs_bucket.functions.in_scope if refs_bucket else []
    funcs = [normalize(f"{func.object_schema}.{func.name}") for func in funcs_in_scope]

    if entry.parse_error or proc_cat.mode == "llm_required":
        needs_llm = True
        statements = None
    else:
        try:
            stmt_refs = extract_refs(entry)
            statements = stmt_refs.statements
            needs_llm = stmt_refs.needs_llm
        except DdlParseError:
            needs_llm = True
            statements = None

    return {
        "params": proc_cat.params,
        "refs": ProcRefs(
            reads_from=sorted(set(reads)),
            writes_to=sorted(set(writes)),
            write_operations=write_ops,
            uses_functions=sorted(set(funcs)),
        ),
        "needs_llm": needs_llm,
        "routing_reasons": proc_cat.routing_reasons,
        "statements": statements,
    }


def _show_view_or_function(
    project_root: Path,
    norm: str,
    type_label: str,
    entry: DdlEntry,
) -> dict[str, Any]:
    cat_loader = load_view_catalog if type_label == "view" else load_function_catalog
    obj_cat = cat_loader(project_root, norm)
    if obj_cat is None:
        _catalog_error(type_label, norm)

    refs_bucket = obj_cat.references
    tables_in_scope = refs_bucket.tables.in_scope if refs_bucket else []
    reads = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_selected]
    writes = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_updated]
    result: dict[str, Any] = {
        "refs": BasicRefs(
            reads_from=sorted(set(reads)),
            writes_to=sorted(set(writes)),
        ),
    }
    if type_label == "view":
        analysis = _analyze_view_select(entry)
        result["sql_elements"] = analysis["sql_elements"]
        result["errors"] = analysis["errors"]
    return result


def run_show(project_root: Path, name: str) -> DiscoverShowOutput:
    """Return the show subcommand result."""
    catalog, _ = _load(project_root)
    found = _find_entry(catalog, name)
    if found is None:
        raise ObjectNotFoundError(normalize(name))

    norm, type_label, entry = found
    if type_label == "table":
        extra = _show_table(project_root, norm)
    elif type_label == "procedure":
        extra = _show_procedure(project_root, norm, entry)
    elif type_label in ("view", "function"):
        extra = _show_view_or_function(project_root, norm, type_label, entry)
    else:
        extra = {}

    base = {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": [],
        "params": [],
        "refs": None,
        "routing_reasons": [],
        "statements": None,
        "needs_llm": None,
        "errors": [],
        "parse_error": entry.parse_error,
    }
    base.update(extra)
    return DiscoverShowOutput(**base)


def _run_refs_from_catalog(project_root: Path, target: str) -> DiscoverRefsOutput:
    _loaders: list[tuple[str, Any]] = [
        ("table", load_table_catalog),
        ("view", load_view_catalog),
        ("function", load_function_catalog),
    ]
    cat: dict[str, Any] | None = None
    object_type = "object"
    for type_label, loader in _loaders:
        cat = loader(project_root, target)
        if cat is not None:
            object_type = type_label
            break

    if cat is None:
        return DiscoverRefsOutput(
            name=target,
            source="catalog",
            readers=[],
            writers=[],
            error=f"no catalog file for {target} — it may not exist in the extracted schemas",
        )

    ref_by = cat.referenced_by
    readers: list[str] = []
    writer_entries: list[WriterEntry] = []
    if ref_by is not None:
        for bucket_type in ("procedures", "views", "functions"):
            scoped = getattr(ref_by, bucket_type, None)
            if scoped is None:
                continue
            for entry in scoped.in_scope:
                fqn = normalize(f"{entry.object_schema}.{entry.name}")
                if entry.is_updated:
                    writer_entries.append(WriterEntry(
                        procedure=fqn,
                        is_selected=entry.is_selected,
                        is_insert_all=entry.is_insert_all,
                    ))
                if entry.is_selected and not entry.is_updated:
                    readers.append(fqn)

    return DiscoverRefsOutput(
        name=target,
        type=object_type,
        source="catalog",
        readers=sorted(set(readers)),
        writers=sorted(writer_entries, key=lambda writer: writer.procedure),
    )


def run_refs(project_root: Path, name: str) -> DiscoverRefsOutput:
    """Return the refs subcommand result."""
    if not has_catalog(project_root):
        raise CatalogNotFoundError(project_root)
    target = normalize(name)

    catalog, _ = _load(project_root)
    found = _find_entry(catalog, name)
    if found is not None:
        _, type_label, _ = found
        if type_label == "procedure":
            return DiscoverRefsOutput(
                error=f"{target} is a procedure — refs only works for tables, views, and functions. Use 'show {name}' to see what this procedure reads/writes.",
            )

    result = _run_refs_from_catalog(project_root, target)
    logger.info("event=refs_complete target=%s source=catalog", target)
    return result
