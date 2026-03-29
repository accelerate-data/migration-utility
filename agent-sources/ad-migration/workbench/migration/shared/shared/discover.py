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

from shared.catalog import (
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    load_function_catalog,
)
from shared.loader import (  # noqa: E402
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    _read_manifest,
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


def _load(ddl_path: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog from flat or indexed directory.

    Returns (catalog, dialect) where dialect is read from manifest.json
    (default: tsql).
    """
    manifest = _read_manifest(ddl_path)
    dialect = manifest["dialect"]
    catalog_json = ddl_path / "catalog.json"
    if catalog_json.exists():
        print(f"discover: loading indexed catalog from {ddl_path}", file=sys.stderr)
        return load_catalog(ddl_path), dialect
    print(f"discover: loading flat directory from {ddl_path}", file=sys.stderr)
    return load_directory(ddl_path, dialect=dialect), dialect


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


def _bucket(catalog: DdlCatalog, object_type: ObjectType) -> dict[str, DdlEntry]:
    return getattr(catalog, object_type.value)


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


def _extract_columns(entry: DdlEntry, dialect: str = "tsql") -> list[dict[str, str]]:
    """Walk ColumnDef nodes in the AST and return column definitions.

    Returns an empty list if the AST is None or no ColumnDef nodes are found.
    """
    if entry.ast is None:
        return []
    columns: list[dict[str, str]] = []
    for col_def in entry.ast.find_all(exp.ColumnDef):
        col_name = col_def.name
        dtype_node = col_def.args.get("kind")
        sql_type = dtype_node.sql(dialect=dialect) if dtype_node is not None else ""
        columns.append({"name": col_name, "sql_type": sql_type})
    return columns


def _extract_params(entry: DdlEntry, dialect: str = "tsql") -> list[dict[str, Any]]:
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
        sql_type = dtype_node.sql(dialect=dialect) if dtype_node is not None else ""
        default_node = param.args.get("default")
        default_val = default_node.sql(dialect=dialect) if default_node is not None else None
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


def run_list(ddl_path: Path, object_type: ObjectType) -> dict[str, Any]:
    """Return the list subcommand result dict."""
    catalog, _ = _load(ddl_path)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return {"objects": objects}


def run_show(ddl_path: Path, name: str) -> dict[str, Any]:
    """Return the show subcommand result dict.

    Raises SystemExit(1) if the object is not found.
    """
    catalog, dialect = _load(ddl_path)
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
        columns = _extract_columns(entry, dialect=dialect)

    needs_llm = False
    classification: str | None = None
    statements: list[dict] | None = None

    if type_label == "procedure":
        params = _extract_params(entry, dialect=dialect)

        # Read reference data from catalog
        proc_cat = load_proc_catalog(ddl_path, norm)
        if proc_cat is not None:
            cat_refs = proc_cat.get("references", {})
            tables_in_scope = cat_refs.get("tables", {}).get("in_scope", [])
            reads = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_selected")]
            writes = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_updated")]
            write_ops: dict[str, list[str]] = {}
            for t in tables_in_scope:
                if t.get("is_updated"):
                    tfqn = normalize(f"{t['schema']}.{t['name']}")
                    ops = ["UPDATE"]
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
            print(f"discover: show refs from catalog for {norm}", file=sys.stderr)

        # Always AST-parse for statements, needs_llm, classification
        try:
            obj_refs_for_stmts = extract_refs(entry)
            needs_llm = obj_refs_for_stmts.needs_llm
            statements = obj_refs_for_stmts.statements
        except DdlParseError:
            needs_llm = True

        if needs_llm or parse_error:
            classification = "claude_assisted"
        else:
            classification = "deterministic"

    elif type_label in ("view", "function"):
        cat_loader = load_view_catalog if type_label == "view" else load_function_catalog
        obj_cat = cat_loader(ddl_path, norm)
        if obj_cat is not None:
            cat_refs = obj_cat.get("references", {})
            tables_in_scope = cat_refs.get("tables", {}).get("in_scope", [])
            reads = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_selected")]
            writes = [normalize(f"{t['schema']}.{t['name']}") for t in tables_in_scope if t.get("is_updated")]
            refs_dict = {
                "reads_from": sorted(set(reads)),
                "writes_to": sorted(set(writes)),
            }
            print(f"discover: show refs from catalog for {norm}", file=sys.stderr)

    return {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": columns,
        "params": params,
        "refs": refs_dict,
        "statements": statements,
        "needs_llm": needs_llm,
        "classification": classification,
        "parse_error": parse_error,
    }


def _run_refs_from_catalog(ddl_path: Path, target: str) -> dict[str, Any]:
    """Build refs result from catalog JSON files (no AST, no BFS, no confidence)."""
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

    Reads ``catalog/tables/<name>.json`` → ``referenced_by`` for instant
    writer identification. No AST, no BFS, no confidence scoring.

    Requires catalog files from setup-ddl.
    """
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
