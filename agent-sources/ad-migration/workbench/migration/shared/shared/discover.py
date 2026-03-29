"""discover.py — DDL object catalog reader.

Standalone CLI with three subcommands:

    list   List all objects of a given type in a DDL directory.
    show   Show details (columns/params/refs) for a single named object.
    refs   Find all procedures/views that reference a given object.

Requires catalog files from setup-ddl. Errors if catalog is missing.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (object not found, no catalog file, etc.)
    2  IO or parse error
"""

from __future__ import annotations

import json
import sys
from enum import Enum
from pathlib import Path
from typing import Any

import typer

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    load_function_catalog,
)
from shared.loader import (
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    _read_manifest,
    extract_refs,
    load_catalog,
    load_directory,
)
from shared.name_resolver import normalize

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ───────────────────────────────────────────────────────────────────


class ObjectType(str, Enum):
    tables = "tables"
    procedures = "procedures"
    views = "views"
    functions = "functions"


def _load(ddl_path: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a DDL directory.

    Requires a catalog/ directory (from setup-ddl). Errors if missing.
    """
    manifest = _read_manifest(ddl_path)
    dialect = manifest["dialect"]
    if not has_catalog(ddl_path):
        print(f"discover: no catalog/ directory in {ddl_path} — run setup-ddl first", file=sys.stderr)
        raise typer.Exit(code=2)
    catalog_json = ddl_path / "catalog.json"
    if catalog_json.exists():
        return load_catalog(ddl_path), dialect
    return load_directory(ddl_path, dialect=dialect), dialect


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


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


def _catalog_error(type_label: str, norm: str) -> None:
    """Print error and raise Exit(1) for missing catalog file."""
    print(
        f"discover: no catalog file for {type_label} {norm} — run setup-ddl first",
        file=sys.stderr,
    )
    raise typer.Exit(code=1)


# ── Core logic (importable for testing) ───────────────────────────────────────


def run_list(ddl_path: Path, object_type: ObjectType) -> dict[str, Any]:
    """Return the list subcommand result dict."""
    catalog, _ = _load(ddl_path)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return {"objects": objects}


def _show_table(ddl_path: Path, norm: str) -> dict[str, Any]:
    """Build show fields for a table object."""
    table_cat = load_table_catalog(ddl_path, norm)
    if table_cat is None:
        _catalog_error("table", norm)
    return {"columns": table_cat.get("columns", [])}


def _show_procedure(
    ddl_path: Path, norm: str, entry: DdlEntry,
) -> dict[str, Any]:
    """Build show fields for a procedure object."""
    proc_cat = load_proc_catalog(ddl_path, norm)
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

    needs_llm = proc_cat.get("needs_llm", False)
    parse_error = entry.parse_error

    if needs_llm or parse_error:
        classification = "claude_assisted"
        statements = None
    else:
        try:
            obj_refs_for_stmts = extract_refs(entry)
            statements = obj_refs_for_stmts.statements
        except DdlParseError:
            statements = None
        classification = "deterministic"

    return {
        "params": params,
        "refs": refs_dict,
        "needs_llm": needs_llm,
        "classification": classification,
        "statements": statements,
    }


def _show_view_or_function(
    ddl_path: Path, norm: str, type_label: str,
) -> dict[str, Any]:
    """Build show fields for a view or function object."""
    cat_loader = load_view_catalog if type_label == "view" else load_function_catalog
    obj_cat = cat_loader(ddl_path, norm)
    if obj_cat is None:
        _catalog_error(type_label, norm)

    cat_refs = obj_cat.get("references", {})
    tables_in_scope = cat_refs.get("tables", {}).get("in_scope", [])
    reads = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_selected")]
    writes = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_updated")]
    return {
        "refs": {
            "reads_from": sorted(set(reads)),
            "writes_to": sorted(set(writes)),
        },
    }


def run_show(ddl_path: Path, name: str) -> dict[str, Any]:
    """Return the show subcommand result dict.

    Reads all metadata from catalog files. AST parsing is only used for
    statement breakdown on deterministic (needs_llm=false) procedures.
    """
    catalog, _ = _load(ddl_path)
    found = _find_entry(catalog, name)
    if found is None:
        norm = normalize(name)
        print(f"discover: object not found: {norm}", file=sys.stderr)
        raise typer.Exit(code=1)

    norm, type_label, entry = found

    if type_label == "table":
        extra = _show_table(ddl_path, norm)
    elif type_label == "procedure":
        extra = _show_procedure(ddl_path, norm, entry)
    elif type_label in ("view", "function"):
        extra = _show_view_or_function(ddl_path, norm, type_label)
    else:
        extra = {}

    return {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": [],
        "params": [],
        "refs": None,
        "statements": None,
        "needs_llm": False,
        "classification": None,
        "parse_error": entry.parse_error,
        **extra,
    }


def _run_refs_from_catalog(ddl_path: Path, target: str) -> dict[str, Any]:
    """Build refs result from catalog JSON files."""
    table_cat = load_table_catalog(ddl_path, target)
    if table_cat is None:
        print(f"discover: no catalog found for {target} — run setup-ddl first", file=sys.stderr)
        raise typer.Exit(code=1)

    ref_by = table_cat.get("referenced_by", {})
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
        "source": "catalog",
        "readers": sorted(set(readers)),
        "writers": sorted(writers, key=lambda w: w["procedure"]),
    }


def run_refs(ddl_path: Path, name: str) -> dict[str, Any]:
    """Return the refs subcommand result dict.

    Reads catalog/tables/<name>.json -> referenced_by for instant
    writer identification. Requires catalog files from setup-ddl.
    """
    if not has_catalog(ddl_path):
        print(f"discover: no catalog/ directory in {ddl_path} — run setup-ddl first", file=sys.stderr)
        raise typer.Exit(code=2)
    target = normalize(name)
    result = _run_refs_from_catalog(ddl_path, target)
    print(f"discover: refs from catalog for {target}", file=sys.stderr)
    return result


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command(name="list")
def list_objects(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    type: ObjectType = typer.Option(..., help="Object type to list"),
) -> None:
    """List all objects of a given type in a DDL directory."""
    try:
        result = run_list(ddl_path, type)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def show(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Show details for a single named DDL object."""
    try:
        result = run_show(ddl_path, name)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def refs(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Find all procedures/views that reference a given object."""
    try:
        result = run_refs(ddl_path, name)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


if __name__ == "__main__":
    app()
