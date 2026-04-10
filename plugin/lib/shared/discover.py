"""discover.py — DDL object catalog reader.

Standalone CLI with six subcommands:

    list             List all objects of a given type in a DDL directory.
    show             Show details (columns/params/refs) for a single named object.
    refs             Find all procedures/views that reference a given object.
    write-statements Persist resolved statements into a procedure catalog file.
    write-scoping    Persist scoping results into a table catalog file.
    write-source     Set or clear the is_source flag on a table catalog file.

Requires catalog files from setup-ddl. Errors if catalog is missing.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (object not found, no catalog file, etc.)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn, Optional

from shared.output_models.discover import (
    BasicRefs,
    DiscoverListOutput,
    DiscoverRefsOutput,
    DiscoverShowOutput,
    ProcRefs,
    WriterEntry,
)

import typer

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    load_function_catalog,
    resolve_catalog_path,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    ObjectNotFoundError,
    extract_refs,
    load_ddl,
)
from shared.cli_utils import emit
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ───────────────────────────────────────────────────────────────────


class ObjectType(str, Enum):
    tables = "tables"
    procedures = "procedures"
    views = "views"
    functions = "functions"


# Re-exports from catalog_writer.py — consumed by CLI handlers below
# and by test_discover.py (via discover.run_write_* calls).
from shared.catalog_writer import (  # noqa: F401
    run_write_scoping,
    run_write_source,
    run_write_statements,
    run_write_table_slice,
    run_write_view_scoping,
)


def _load(project_root: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a DDL directory.

    Requires a catalog/ directory (from setup-ddl).

    Raises:
        CatalogNotFoundError: if catalog/ directory is missing.
    """
    return load_ddl(project_root)


def _bucket(catalog: DdlCatalog, object_type: ObjectType) -> dict[str, DdlEntry]:
    return getattr(catalog, object_type.value)


def _find_entry(
    catalog: DdlCatalog, name: str,
) -> tuple[str, str, DdlEntry] | None:
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
    """Raise CatalogFileMissingError for missing catalog file."""
    raise CatalogFileMissingError(type_label, norm)


def _is_view_catalog_path(cat_path: Path) -> bool:
    """Return True when a catalog path points at catalog/views/."""
    return cat_path.parent.name == "views"


# ── Core logic (importable for testing) ───────────────────────────────────────


def run_list(project_root: Path, object_type: ObjectType) -> DiscoverListOutput:
    """Return the list subcommand result."""
    catalog, _ = _load(project_root)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return DiscoverListOutput(objects=objects)


def _show_table(project_root: Path, norm: str) -> dict[str, Any]:
    """Build show fields for a table object."""
    table_cat = load_table_catalog(project_root, norm)
    if table_cat is None:
        _catalog_error("table", norm)
    return {"columns": table_cat.columns}


def _show_procedure(
    project_root: Path, norm: str, entry: DdlEntry,
) -> dict[str, Any]:
    """Build show fields for a procedure object."""
    proc_cat = load_proc_catalog(project_root, norm)
    if proc_cat is None:
        _catalog_error("procedure", norm)

    params = proc_cat.params

    refs_bucket = proc_cat.references
    tables_in_scope = refs_bucket.tables.in_scope if refs_bucket else []
    reads = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_selected]
    writes = [normalize(f"{t.object_schema}.{t.name}") for t in tables_in_scope if t.is_updated]
    write_ops: dict[str, list[str]] = {}
    for t in tables_in_scope:
        if t.is_updated:
            tfqn = normalize(f"{t.object_schema}.{t.name}")
            ops = ["WRITE"]
            if t.is_insert_all:
                ops.append("INSERT")
            write_ops[tfqn] = ops
    funcs_in_scope = refs_bucket.functions.in_scope if refs_bucket else []
    funcs = [normalize(f"{f.object_schema}.{f.name}") for f in funcs_in_scope]
    refs_model = ProcRefs(
        reads_from=sorted(set(reads)),
        writes_to=sorted(set(writes)),
        write_operations=write_ops,
        uses_functions=sorted(set(funcs)),
    )

    routing_mode = proc_cat.mode
    routing_reasons = proc_cat.routing_reasons
    parse_error = entry.parse_error

    if parse_error:
        needs_llm = True
        statements = None
    elif routing_mode == "llm_required":
        needs_llm = True
        statements = None
    else:
        try:
            obj_refs_for_stmts = extract_refs(entry)
            statements = obj_refs_for_stmts.statements
            needs_llm = obj_refs_for_stmts.needs_llm
        except DdlParseError:
            needs_llm = True
            statements = None

    return {
        "params": params,
        "refs": refs_model,
        "needs_llm": needs_llm,
        "routing_reasons": routing_reasons,
        "statements": statements,
    }


from shared.view_analysis import _analyze_view_select


def _show_view_or_function(
    project_root: Path, norm: str, type_label: str, entry: "DdlEntry",
) -> dict[str, Any]:
    """Build show fields for a view or function object."""
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
    """Return the show subcommand result.

    Reads all metadata from catalog files. AST parsing is only used for
    statement breakdown on deterministic (needs_llm=false) procedures.
    """
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
    """Build refs result from catalog JSON files.

    Looks up tables, views, and functions — any object that can be
    referenced by a procedure, view, or function.
    """
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
        writers=sorted(writer_entries, key=lambda w: w.procedure),
    )


def run_refs(project_root: Path, name: str) -> DiscoverRefsOutput:
    """Return the refs subcommand result.

    Reads catalog/tables/<name>.json -> referenced_by for instant
    writer identification. Requires catalog files from setup-ddl.
    """
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


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command(name="list")
def list_objects(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    type: ObjectType = typer.Option(..., help="Object type to list"),
) -> None:
    """List all objects of a given type in a DDL directory."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_list(project_root, type)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def show(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Show details for a single named DDL object."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_show(project_root, name)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def refs(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Find all procedures/views that reference a given object."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_refs(project_root, name)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command(name="write-statements")
def write_statements(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified procedure name (schema.Name)"),
    statements: str = typer.Option("", help="JSON array of resolved statement objects"),
    statements_file: Optional[Path] = typer.Option(None, "--statements-file", help="Path to file containing statements JSON"),
) -> None:
    """Persist resolved statements into a procedure catalog file."""
    if statements_file:
        statements = statements_file.read_text(encoding="utf-8")
    if not statements:
        logger.error("event=command_failed error=no statements provided (use --statements or --statements-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        stmts = json.loads(statements)
    except json.JSONDecodeError as exc:
        logger.error("event=command_failed error=invalid_json detail=%s", exc)
        raise typer.Exit(code=2) from exc
    try:
        result = run_write_statements(project_root, name, stmts)
    except (ObjectNotFoundError, FileNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-scoping")
def write_scoping(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully qualified table name"),
    scoping: str = typer.Option("", help="Scoping JSON"),
    scoping_file: Optional[Path] = typer.Option(None, "--scoping-file", help="Path to file containing scoping JSON"),
) -> None:
    """Persist scoping results to a table catalog file."""
    if scoping_file:
        scoping = scoping_file.read_text(encoding="utf-8")
    if not scoping:
        logger.error("event=command_failed error=no scoping provided (use --scoping or --scoping-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        scoping_data = json.loads(scoping)
    except json.JSONDecodeError as exc:
        logger.error("event=command_failed error=invalid_json detail=%s", exc)
        raise typer.Exit(code=2) from exc
    try:
        # Auto-detect: route to view or table scoping based on catalog presence
        cat_path = resolve_catalog_path(project_root, normalize(name))
        if _is_view_catalog_path(cat_path):
            result = run_write_view_scoping(project_root, name, scoping_data)
        else:
            result = run_write_scoping(project_root, name, scoping_data)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-source")
def write_source(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully qualified table name"),
    value: bool = typer.Option(True, "--value/--no-value", help="Set (--value) or clear (--no-value) the is_source flag"),
) -> None:
    """Set or clear the is_source flag on a table catalog file.

    Marks the table as a dbt source (is_source: true) or resets it to false.
    Guard: table catalog must exist and scoping must be present.
    """
    project_root = resolve_project_root(project_root)
    try:
        result = run_write_source(project_root, name, value)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-slice")
def write_slice(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    proc: str = typer.Option(..., "--proc", help="Fully qualified procedure FQN"),
    table: str = typer.Option(..., "--table", help="Fully qualified table FQN"),
    slice: Optional[str] = typer.Option(None, "--slice", help="Inline DDL text for the table slice (mutually exclusive with --slice-file)"),
    slice_file: Optional[Path] = typer.Option(None, "--slice-file", help="Path to file containing the DDL slice (mutually exclusive with --slice)"),
) -> None:
    """Write a per-table DDL slice into a procedure catalog file."""
    if slice_file is not None and slice is not None:
        logger.error("event=command_failed error=--slice and --slice-file are mutually exclusive")
        raise typer.Exit(code=1)
    if slice_file:
        slice = slice_file.read_text(encoding="utf-8")
    if not slice:
        logger.error("event=command_failed error=no slice provided (use --slice or --slice-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        result = run_write_table_slice(project_root, proc, table, slice)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
