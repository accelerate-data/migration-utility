"""discover.py — DDL object catalog reader.

Standalone CLI with five subcommands:

    list             List all objects of a given type in a DDL directory.
    show             Show details (columns/params/refs) for a single named object.
    refs             Find all procedures/views that reference a given object.
    write-statements Persist resolved statements into a procedure catalog file.
    write-scoping    Persist scoping results into a table catalog file.

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

import typer

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    load_function_catalog,
    write_json,
    write_proc_statements,
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


# ── Core logic (importable for testing) ───────────────────────────────────────


def run_list(project_root: Path, object_type: ObjectType) -> dict[str, Any]:
    """Return the list subcommand result dict."""
    catalog, _ = _load(project_root)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return {"objects": objects}


def _show_table(project_root: Path, norm: str) -> dict[str, Any]:
    """Build show fields for a table object."""
    table_cat = load_table_catalog(project_root, norm)
    if table_cat is None:
        _catalog_error("table", norm)
    return {"columns": table_cat.get("columns", [])}


def _show_procedure(
    project_root: Path, norm: str, entry: DdlEntry,
) -> dict[str, Any]:
    """Build show fields for a procedure object."""
    proc_cat = load_proc_catalog(project_root, norm)
    if proc_cat is None:
        _catalog_error("procedure", norm)

    params = proc_cat.get("params", [])

    cat_refs = proc_cat.get("references", {})
    tables_in_scope = cat_refs.get("tables", {}).get("in_scope", [])
    reads = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_selected")]
    writes = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_updated")]
    write_ops: dict[str, list[str]] = {}
    for t in tables_in_scope:
        if t.get("is_updated"):
            tfqn = normalize(f"{t['schema']}.{t['name']}")
            ops = ["WRITE"]
            if t.get("is_insert_all"):
                ops.append("INSERT")
            write_ops[tfqn] = ops
    funcs = [normalize(f"{f['schema']}.{f['name']}") for f in cat_refs.get("functions", {}).get("in_scope", [])]
    refs_dict = {
        "reads_from": sorted(set(reads)),
        "writes_to": sorted(set(writes)),
        "write_operations": write_ops,
        "uses_functions": sorted(set(funcs)),
    }

    routing_mode = proc_cat.get("mode")
    routing_reasons = proc_cat.get("routing_reasons", [])
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
        "refs": refs_dict,
        "needs_llm": needs_llm,
        "routing_reasons": routing_reasons,
        "statements": statements,
    }


def _analyze_view_select(entry: "DdlEntry") -> dict[str, Any]:
    """Extract SQL elements from a view's AST. Returns sql_elements, needs_llm, errors."""
    import sqlglot.expressions as exp

    if entry.parse_error or entry.ast is None:
        return {
            "sql_elements": None,
            "needs_llm": True,
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": entry.parse_error or "AST is None"}],
        }

    try:
        elements: list[dict[str, Any]] = []
        ast = entry.ast

        # JOINs
        for join in ast.find_all(exp.Join):
            join_type = "JOIN"
            if join.args.get("kind"):
                kind = str(join.args["kind"]).upper()
                side = str(join.args.get("side", "")).upper()
                join_type = f"{side} {kind}".strip() if side else kind
            table = join.this
            target = table.name if hasattr(table, "name") else str(table)
            if table.args.get("db"):
                target = f"{table.args['db']}.{target}"
            elements.append({"type": "join", "detail": f"{join_type} {target}"})

        # CTEs (WITH clause)
        for cte in ast.find_all(exp.With):
            elements.append({"type": "cte", "detail": f"{len(list(cte.find_all(exp.CTE)))} CTE(s)"})
            break  # only report once

        # GROUP BY
        for _ in ast.find_all(exp.Group):
            elements.append({"type": "group_by", "detail": "GROUP BY"})
            break

        # Aggregation functions
        agg_funcs: list[str] = []
        for agg in ast.find_all(exp.AggFunc):
            name = type(agg).__name__.upper()
            if name not in agg_funcs:
                agg_funcs.append(name)
        if agg_funcs:
            elements.append({"type": "aggregation", "detail": ", ".join(sorted(agg_funcs))})

        # Window functions (OVER)
        for _ in ast.find_all(exp.Window):
            elements.append({"type": "window_function", "detail": "OVER clause"})
            break

        # CASE expressions
        for _ in ast.find_all(exp.Case):
            elements.append({"type": "case", "detail": "CASE expression"})
            break

        # Subqueries
        subquery_count = sum(1 for _ in ast.find_all(exp.Subquery))
        if subquery_count:
            elements.append({"type": "subquery", "detail": f"{subquery_count} subquery(ies)"})

        return {"sql_elements": elements, "needs_llm": False, "errors": []}

    except Exception as exc:  # noqa: BLE001
        return {
            "sql_elements": None,
            "needs_llm": True,
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": str(exc)}],
        }


def _show_view_or_function(
    project_root: Path, norm: str, type_label: str, entry: "DdlEntry",
) -> dict[str, Any]:
    """Build show fields for a view or function object."""
    cat_loader = load_view_catalog if type_label == "view" else load_function_catalog
    obj_cat = cat_loader(project_root, norm)
    if obj_cat is None:
        _catalog_error(type_label, norm)

    cat_refs = obj_cat.get("references", {})
    tables_in_scope = cat_refs.get("tables", {}).get("in_scope", [])
    reads = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_selected")]
    writes = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_updated")]
    result: dict[str, Any] = {
        "refs": {
            "reads_from": sorted(set(reads)),
            "writes_to": sorted(set(writes)),
        },
    }

    if type_label == "view":
        analysis = _analyze_view_select(entry)
        result["sql_elements"] = analysis["sql_elements"]
        result["needs_llm"] = analysis["needs_llm"]
        result["errors"] = analysis["errors"]

    return result


def run_show(project_root: Path, name: str) -> dict[str, Any]:
    """Return the show subcommand result dict.

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

    return {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": [],
        "params": [],
        "refs": None,
        "routing_reasons": [],
        "statements": None,
        "needs_llm": None,
        "parse_error": entry.parse_error,
        **extra,
    }


def _run_refs_from_catalog(project_root: Path, target: str) -> dict[str, Any]:
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
        return {
            "name": target,
            "source": "catalog",
            "readers": [],
            "writers": [],
            "error": f"no catalog file for {target} — it may not exist in the extracted schemas",
        }

    ref_by = cat.get("referenced_by", {})
    readers: list[str] = []
    writers: list[dict[str, Any]] = []

    for bucket_type in ("procedures", "views", "functions"):
        for entry in ref_by.get(bucket_type, {}).get("in_scope", []):
            fqn = normalize(f"{entry['schema']}.{entry['name']}")
            is_updated = entry.get("is_updated", False)
            is_selected = entry.get("is_selected", False)

            if is_updated:
                writers.append({
                    "procedure": fqn,
                    "write_type": "direct",
                    "is_updated": True,
                    "is_selected": is_selected,
                    "is_insert_all": entry.get("is_insert_all", False),
                })
            if is_selected and not is_updated:
                readers.append(fqn)

    return {
        "name": target,
        "type": object_type,
        "source": "catalog",
        "readers": sorted(set(readers)),
        "writers": sorted(writers, key=lambda w: w["procedure"]),
    }


def run_write_statements(
    project_root: Path, name: str, statements: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist resolved statements into a procedure catalog file.

    All statements must have ``action`` set to ``migrate`` or ``skip`` —
    unresolved ``needs_llm`` actions are rejected.

    Returns a dict with ``written`` (path) and ``statement_count``.
    """
    for stmt in statements:
        action = stmt.get("action")
        if action not in ("migrate", "skip"):
            raise ValueError(
                f"Unresolved statement action {action!r} for {stmt.get('id', '?')} — "
                "all actions must be 'migrate' or 'skip' before writing."
            )
    path = write_proc_statements(project_root, name, statements)
    return {"written": str(path), "statement_count": len(statements)}


def run_write_scoping(
    project_root: Path,
    table_fqn: str,
    scoping: dict[str, Any],
) -> dict[str, Any]:
    """Validate and merge scoping results into a table catalog file."""
    table_norm = normalize(table_fqn)

    # Validate status
    valid_statuses = {"resolved", "ambiguous_multi_writer", "no_writer_found", "error"}
    status = scoping.get("status", "")
    if status not in valid_statuses:
        raise ValueError(f"Invalid scoping status: {status!r}")

    # Validate selected_writer present when resolved
    if status == "resolved" and not scoping.get("selected_writer"):
        raise ValueError("selected_writer required when status is resolved")

    # Load existing catalog
    cat = load_table_catalog(project_root, table_norm)
    if cat is None:
        raise CatalogFileMissingError("table", table_norm)

    # Merge scoping section
    cat["scoping"] = scoping

    catalog_dir = resolve_catalog_dir(project_root) / "tables"
    cat_path = catalog_dir / f"{table_norm}.json"
    write_json(cat_path, cat)

    return {"written": str(cat_path), "status": "ok"}


def run_write_view_scoping(
    project_root: Path,
    view_fqn: str,
    scoping: dict[str, Any],
) -> dict[str, Any]:
    """Validate and merge scoping results into a view catalog file."""
    view_norm = normalize(view_fqn)

    valid_statuses = {"analyzed", "error"}
    status = scoping.get("status", "")
    if status not in valid_statuses:
        raise ValueError(f"Invalid view scoping status: {status!r}")

    cat = load_view_catalog(project_root, view_norm)
    if cat is None:
        raise CatalogFileMissingError("view", view_norm)

    cat["scoping"] = scoping

    catalog_dir = resolve_catalog_dir(project_root) / "views"
    cat_path = catalog_dir / f"{view_norm}.json"
    write_json(cat_path, cat)

    return {"written": str(cat_path), "status": "ok"}


def run_refs(project_root: Path, name: str) -> dict[str, Any]:
    """Return the refs subcommand result dict.

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
            return {
                "error": f"{target} is a procedure — refs only works for tables, views, and functions. Use 'show {name}' to see what this procedure reads/writes.",
            }

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
        # Auto-detect: check if a view catalog exists for this FQN
        catalog_dir = resolve_catalog_dir(project_root)
        view_cat_path = catalog_dir / "views" / f"{normalize(name)}.json"
        if view_cat_path.exists():
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


if __name__ == "__main__":
    app()
