"""discover.py — DDL object catalog and semantic reference finder.

Standalone CLI with three subcommands:

    list   List all objects of a given type in a DDL directory.
    show   Show details (columns/params/refs) for a single named object.
    refs   Find all procedures/views that reference a given object.

Auto-detects flat vs indexed format: if catalog.json exists in --ddl-path,
uses load_catalog(); otherwise uses load_directory().

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (object not found, etc.)
    2  IO or parse error
"""

from __future__ import annotations

import json
import sys
from enum import Enum
from pathlib import Path
from typing import Any

import sqlglot.expressions as exp
import typer

# Plugin root is one directory above this script; add shared/ to sys.path so the
# shared package is importable when running without editable install.
_PLUGIN_ROOT = Path(__file__).parent
_SHARED_ROOT = _PLUGIN_ROOT / "shared"
if str(_SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(_SHARED_ROOT))

from shared.loader import (  # noqa: E402
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    extract_refs,
    load_catalog,
    load_directory,
)
from shared.name_resolver import normalize  # noqa: E402

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ───────────────────────────────────────────────────────────────────


class ObjectType(str, Enum):
    tables = "tables"
    procedures = "procedures"
    views = "views"
    functions = "functions"


def _load(ddl_path: Path, dialect: str) -> DdlCatalog:
    """Load a DdlCatalog from flat or indexed directory."""
    catalog_json = ddl_path / "catalog.json"
    if catalog_json.exists():
        print(f"discover: loading indexed catalog from {ddl_path}", file=sys.stderr)
        return load_catalog(ddl_path)
    print(f"discover: loading flat directory from {ddl_path}", file=sys.stderr)
    return load_directory(ddl_path, dialect=dialect)


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


def _bucket(catalog: DdlCatalog, object_type: ObjectType) -> dict[str, DdlEntry]:
    return getattr(catalog, object_type.value)


def _singular(object_type: ObjectType) -> str:
    return object_type.value.rstrip("s")


def _find_entry(
    catalog: DdlCatalog, name: str
) -> tuple[str, str, DdlEntry] | None:
    """Find an entry by normalized name across all buckets.

    Returns (normalized_name, type_label, entry) or None.
    """
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


# ── Column / param extraction from AST ───────────────────────────────────────


def _extract_columns(entry: DdlEntry) -> list[dict[str, str]]:
    """Walk ColumnDef nodes in the AST and return column definitions.

    Returns an empty list if the AST is None or no ColumnDef nodes are found.
    """
    if entry.ast is None:
        return []
    columns: list[dict[str, str]] = []
    for col_def in entry.ast.find_all(exp.ColumnDef):
        col_name = col_def.name
        dtype_node = col_def.args.get("kind")
        sql_type = dtype_node.sql(dialect="tsql") if dtype_node is not None else ""
        columns.append({"name": col_name, "sql_type": sql_type})
    return columns


def _extract_params(entry: DdlEntry) -> list[dict[str, Any]]:
    """Extract procedure parameters from the AST.

    Walks ParameterizedTypedef / Parameter nodes attached to the Create node.
    Returns an empty list if the AST is None or no params are found.
    """
    if entry.ast is None:
        return []
    params: list[dict[str, Any]] = []
    for param in entry.ast.find_all(exp.Parameter):
        param_name = param.name
        dtype_node = param.args.get("kind")
        sql_type = dtype_node.sql(dialect="tsql") if dtype_node is not None else ""
        default_node = param.args.get("default")
        default_val = default_node.sql(dialect="tsql") if default_node is not None else None
        is_output = bool(param.args.get("output"))
        params.append(
            {
                "name": param_name,
                "sql_type": sql_type,
                "default": default_val,
                "is_output": is_output,
            }
        )
    return params


# ── Core logic (importable for testing) ───────────────────────────────────────


def run_list(ddl_path: Path, object_type: ObjectType, dialect: str) -> dict[str, Any]:
    """Return the list subcommand result dict."""
    catalog = _load(ddl_path, dialect)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return {"objects": objects}


def run_show(ddl_path: Path, name: str, dialect: str) -> dict[str, Any]:
    """Return the show subcommand result dict.

    Raises SystemExit(1) if the object is not found.
    """
    catalog = _load(ddl_path, dialect)
    found = _find_entry(catalog, name)
    if found is None:
        norm = normalize(name)
        print(f"discover: object not found: {norm}", file=sys.stderr)
        raise typer.Exit(code=1)

    norm, type_label, entry = found

    columns: list[dict] = []
    params: list[dict] = []
    refs_dict: dict | None = None
    parse_error: str | None = entry.parse_error

    if type_label == "table":
        columns = _extract_columns(entry)

    elif type_label == "procedure":
        params = _extract_params(entry)
        if entry.ast is not None:
            try:
                obj_refs = extract_refs(entry)
                refs_dict = {
                    "reads_from": obj_refs.reads_from,
                    "writes_to": obj_refs.writes_to,
                }
            except DdlParseError as exc:
                parse_error = str(exc)
                refs_dict = None

    elif type_label in ("view", "function"):
        if entry.ast is not None:
            try:
                obj_refs = extract_refs(entry)
                refs_dict = {
                    "reads_from": obj_refs.reads_from,
                    "writes_to": obj_refs.writes_to,
                }
            except DdlParseError as exc:
                parse_error = str(exc)
                refs_dict = None

    return {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": columns,
        "params": params,
        "refs": refs_dict,
        "parse_error": parse_error,
    }


def run_refs(ddl_path: Path, name: str, dialect: str) -> dict[str, Any]:
    """Return the refs subcommand result dict."""
    catalog = _load(ddl_path, dialect)
    target = normalize(name)

    # Iterate all procs and views, check if they reference target
    referenced_by: list[str] = []
    for bucket_name in ("procedures", "views"):
        bucket: dict[str, DdlEntry] = getattr(catalog, bucket_name)
        for caller_name, entry in bucket.items():
            if entry.parse_error is not None:
                print(
                    f"discover: skipping {caller_name} (parse_error: {entry.parse_error[:60]})",
                    file=sys.stderr,
                )
                continue
            try:
                obj_refs = extract_refs(entry)
                all_refs = set(obj_refs.reads_from) | set(obj_refs.writes_to)
                if target in all_refs:
                    referenced_by.append(caller_name)
            except DdlParseError as exc:
                print(
                    f"discover: skipping {caller_name} (extract_refs error: {str(exc)[:60]})",
                    file=sys.stderr,
                )

    return {"name": target, "referenced_by": sorted(referenced_by)}


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command()
def list_objects(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    type: ObjectType = typer.Option(..., help="Object type to list"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
) -> None:
    """List all objects of a given type in a DDL directory."""
    try:
        result = run_list(ddl_path, type, dialect)
    except FileNotFoundError as exc:
        print(f"discover: IO error: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def show(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
) -> None:
    """Show details for a single named DDL object."""
    try:
        result = run_show(ddl_path, name, dialect)
    except FileNotFoundError as exc:
        print(f"discover: IO error: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def refs(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
) -> None:
    """Find all procedures/views that reference a given object."""
    try:
        result = run_refs(ddl_path, name, dialect)
    except FileNotFoundError as exc:
        print(f"discover: IO error: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


if __name__ == "__main__":
    app()
