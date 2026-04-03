"""Catalog JSON file I/O for per-object metadata extracted from sys.* views.

Reads and writes the ``catalog/`` subdirectory that setup-ddl
produces alongside the flat ``.sql`` DDL files.  Each object gets its own JSON
file keyed by normalized ``schema.name``.

Layout::

    <project-root>/
    └── catalog/
        ├── tables/<schema>.<table>.json
        ├── procedures/<schema>.<proc>.json
        ├── views/<schema>.<view>.json
        └── functions/<schema>.<function>.json

Table files carry catalog signals (PKs, FKs, identity, CDC, sensitivity) plus
``referenced_by`` (inbound references flipped from proc/view/function DMF data).
Proc/view/function files carry ``references`` (outbound references from the DMF).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from shared.dmf_processing import empty_scoped
from shared.env_config import resolve_catalog_dir
from shared.name_resolver import fqn_parts, normalize
from shared.tsql_utils import mask_tsql

# ── Routing flag patterns ────────────────────────────────────────────────────

_CONTROL_FLOW_REASONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bIF\b", re.IGNORECASE), "if_else"),
    (re.compile(r"\bWHILE\b", re.IGNORECASE), "while_loop"),
    (re.compile(r"\bBEGIN\s+TRY\b", re.IGNORECASE), "try_catch"),
)

_SELECT_INTO_RE = re.compile(
    r"^(?!.*\bINSERT\b).*\bINTO\s+[\[\w#@]",
    re.IGNORECASE | re.MULTILINE,
)
_TRUNCATE_RE = re.compile(r"\bTRUNCATE\b", re.IGNORECASE)
_STATIC_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?(?!sp_executesql\b)(?!\()"
    r"(?:\[[^\]]+\]|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\w+)){0,3}",
    re.IGNORECASE,
)
_DYNAMIC_EXEC_RE = re.compile(r"\bEXEC(?:UTE)?\s*\(", re.IGNORECASE)
_SP_EXECUTESQL_RE = re.compile(r"\bEXEC(?:UTE)?\s+sp_executesql\b", re.IGNORECASE)
_SP_EXECUTESQL_LITERAL_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+sp_executesql\s+N?'(?:''|[^'])*'",
    re.IGNORECASE | re.DOTALL,
)
_SP_EXECUTESQL_VARIABLE_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+sp_executesql\s+@",
    re.IGNORECASE,
)
_CROSS_DB_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?"
    r"(?!sp_executesql\b)"
    r"(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)"
    r"(?!\s*\.)",
    re.IGNORECASE,
)
_LINKED_SERVER_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?"
    r"(?!sp_executesql\b)"
    r"(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)",
    re.IGNORECASE,
)
# ── Schemas (TypedDict-style, but plain dicts in practice) ──────────────────
#
# We define the shapes here as documentation.  At runtime everything is
# ``dict[str, Any]`` — no Pydantic overhead for what are pure serialisation
# containers.
#
# ReferenceEntry:
#   schema: str
#   name: str
#   is_selected: bool
#   is_updated: bool
#   is_insert_all: bool          (procs only)
#   columns: list[ColumnRef]     (optional)
#
# ColumnRef:
#   name: str
#   is_selected: bool
#   is_updated: bool
#
# TableCatalog:
#   primary_keys: list[PrimaryKey]
#   unique_indexes: list[UniqueIndex]
#   foreign_keys: list[ForeignKey]
#   auto_increment_columns: list[dict]   (column, mechanism, seed?, increment?)
#   change_capture: {enabled: bool, mechanism: str} | None
#   sensitivity_classifications: list[SensitivityEntry]
#   referenced_by: {procedures: [...], views: [...], functions: [...]}
#
# ProcCatalog / ViewCatalog / FunctionCatalog:
#   references: {tables: [...], views: [...], functions: [...], procedures: [...]}


# ── File naming ─────────────────────────────────────────────────────────────


def _catalog_dir(project_root: Path) -> Path:
    return resolve_catalog_dir(project_root)


def _object_path(project_root: Path, object_type: str, fqn: str) -> Path:
    """Return the catalog JSON path for a given object.

    *object_type* is one of ``tables``, ``procedures``, ``views``,
    ``functions``.  *fqn* is a normalised ``schema.name`` string.
    """
    return _catalog_dir(project_root) / object_type / f"{fqn}.json"


def has_catalog(project_root: Path) -> bool:
    """Return True if a catalog directory exists with at least one file."""
    d = _catalog_dir(project_root)
    if not d.is_dir():
        return False
    return any(d.rglob("*.json"))


# ── Loading ─────────────────────────────────────────────────────────────────


def _load_catalog_file(project_root: Path, object_type: str, fqn: str) -> dict[str, Any] | None:
    """Load a single catalog JSON file, or ``None`` if absent."""
    p = _object_path(project_root, object_type, normalize(fqn))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def load_table_catalog(project_root: Path, table_fqn: str) -> dict[str, Any] | None:
    return _load_catalog_file(project_root, "tables", table_fqn)


def load_proc_catalog(project_root: Path, proc_fqn: str) -> dict[str, Any] | None:
    return _load_catalog_file(project_root, "procedures", proc_fqn)


def load_view_catalog(project_root: Path, view_fqn: str) -> dict[str, Any] | None:
    return _load_catalog_file(project_root, "views", view_fqn)


def load_function_catalog(project_root: Path, func_fqn: str) -> dict[str, Any] | None:
    return _load_catalog_file(project_root, "functions", func_fqn)


def read_selected_writer(project_root: Path, table_fqn: str) -> str | None:
    """Read selected_writer from the scoping section of a table catalog file.

    Returns None if the table catalog doesn't exist or has no scoping section
    or scoping.selected_writer is not set.
    """
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        return None
    scoping = cat.get("scoping")
    if scoping is None:
        return None
    return scoping.get("selected_writer")


# ── Routing flag detection ──────────────────────────────────────────────────


def scan_routing_flags(definition: str) -> dict[str, bool]:
    """Scan a proc/view/function body and return routing summary fields.

    Returns the backward-compatible flags plus the canonical routing summary:
    ``{"needs_llm", "needs_enrich", "mode", "routing_reasons"}``.
    """
    masked = mask_tsql(definition)
    masked_for_exec = mask_tsql(definition, mask_bracketed_identifiers=False)
    reasons: list[str] = []

    for pattern, reason in _CONTROL_FLOW_REASONS:
        if pattern.search(masked):
            reasons.append(reason)

    has_dynamic_exec = bool(_DYNAMIC_EXEC_RE.search(masked))
    has_sp_executesql_literal = bool(_SP_EXECUTESQL_LITERAL_RE.search(masked))
    has_sp_executesql_variable = bool(_SP_EXECUTESQL_VARIABLE_RE.search(masked))
    has_static_exec = bool(_STATIC_EXEC_RE.search(definition))
    has_select_into = bool(_SELECT_INTO_RE.search(masked))
    has_truncate = bool(_TRUNCATE_RE.search(masked))
    has_linked_server_exec = bool(_LINKED_SERVER_EXEC_RE.search(definition))
    has_cross_db_exec = bool(_CROSS_DB_EXEC_RE.search(definition)) and not has_linked_server_exec

    if has_dynamic_exec or has_sp_executesql_variable:
        reasons.append("dynamic_sql_variable")
    elif has_sp_executesql_literal or _SP_EXECUTESQL_RE.search(masked):
        reasons.append("dynamic_sql_literal")

    if has_static_exec:
        reasons.append("static_exec")
    if has_cross_db_exec:
        reasons.append("cross_db_exec")
    if has_linked_server_exec:
        reasons.append("linked_server_exec")

    routing_reasons = sorted(set(reasons))
    needs_llm = "dynamic_sql_variable" in routing_reasons
    needs_enrich = has_static_exec or has_select_into or has_truncate

    if needs_llm:
        mode = "llm_required"
    elif "dynamic_sql_literal" in routing_reasons:
        mode = "dynamic_sql_literal"
    elif "static_exec" in routing_reasons and needs_enrich:
        mode = "call_graph_enrich"
    elif any(reason in routing_reasons for reason in ("if_else", "while_loop", "try_catch")):
        mode = "control_flow_fallback"
    else:
        mode = "deterministic"

    return {
        "needs_llm": needs_llm,
        "needs_enrich": needs_enrich,
        "mode": mode,
        "routing_reasons": routing_reasons,
    }


# ── Writing ─────────────────────────────────────────────────────────────────


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    try:
        tmp_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def ensure_references(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a proc/view/function catalog dict has a full references structure."""
    if "references" not in data:
        data["references"] = {
            "tables": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
            "procedures": empty_scoped(),
        }
    refs = data["references"]
    for bucket in ("tables", "views", "functions", "procedures"):
        if bucket not in refs:
            refs[bucket] = empty_scoped()
        if "in_scope" not in refs[bucket]:
            refs[bucket]["in_scope"] = []
        if "out_of_scope" not in refs[bucket]:
            refs[bucket]["out_of_scope"] = []
    return data


def ensure_referenced_by(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a table catalog dict has a full referenced_by structure."""
    if "referenced_by" not in data:
        data["referenced_by"] = {
            "procedures": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
        }
    ref_by = data["referenced_by"]
    for bucket in ("procedures", "views", "functions"):
        if bucket not in ref_by:
            ref_by[bucket] = empty_scoped()
        if "in_scope" not in ref_by[bucket]:
            ref_by[bucket]["in_scope"] = []
        if "out_of_scope" not in ref_by[bucket]:
            ref_by[bucket]["out_of_scope"] = []
    return data


def write_table_catalog(
    project_root: Path,
    table_fqn: str,
    signals: dict[str, Any],
    referenced_by: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
) -> Path:
    """Write a table catalog file.  Returns the written path."""
    fqn = normalize(table_fqn)
    schema, name = fqn_parts(fqn)
    defaults: dict[str, Any] = {
        "columns": [],
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "change_capture": None,
        "sensitivity_classifications": [],
    }
    data: dict[str, Any] = {"schema": schema, "name": name, **defaults, **signals}
    if referenced_by is not None:
        data["referenced_by"] = referenced_by
    else:
        data.setdefault("referenced_by", {
            "procedures": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
        })
    p = _object_path(project_root, "tables", fqn)
    _write_json(p, data)
    return p


def write_proc_statements(
    project_root: Path,
    proc_fqn: str,
    statements: list[dict[str, Any]],
) -> Path:
    """Persist resolved statements into an existing procedure catalog file.

    Reads the current catalog file, sets ``statements``, and writes it back.
    Raises ``FileNotFoundError`` if the catalog file does not exist.
    """
    norm = normalize(proc_fqn)
    p = _object_path(project_root, "procedures", norm)
    if not p.exists():
        raise FileNotFoundError(f"Procedure catalog not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    data["statements"] = statements
    _write_json(p, data)
    return p


def write_object_catalog(
    project_root: Path,
    object_type: str,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    needs_llm: bool = False,
    needs_enrich: bool = False,
    mode: str | None = None,
    routing_reasons: list[str] | None = None,
    params: list[dict[str, Any]] | None = None,
) -> Path:
    """Write a proc/view/function catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references}
    if params is not None:
        data["params"] = params
    if needs_llm:
        data["needs_llm"] = True
    if needs_enrich:
        data["needs_enrich"] = True
    if mode is not None:
        data["mode"] = mode
    if routing_reasons is not None:
        data["routing_reasons"] = routing_reasons
    p = _object_path(project_root, object_type, norm)
    _write_json(p, data)
    return p
