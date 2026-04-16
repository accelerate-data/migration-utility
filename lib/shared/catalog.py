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
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from shared.catalog_models import (
    FunctionCatalog,
    ProcedureCatalog,
    TableCatalog,
    ViewCatalog,
)
from shared.dmf_processing import empty_scoped
from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogFileMissingError, CatalogLoadError
from shared.name_resolver import fqn_parts, normalize


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
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(p), exc) from exc


def load_table_catalog(project_root: Path, table_fqn: str) -> TableCatalog | None:
    data = _load_catalog_file(project_root, "tables", table_fqn)
    return TableCatalog.model_validate(data) if data is not None else None


def load_proc_catalog(project_root: Path, proc_fqn: str) -> ProcedureCatalog | None:
    data = _load_catalog_file(project_root, "procedures", proc_fqn)
    return ProcedureCatalog.model_validate(data) if data is not None else None


def load_view_catalog(project_root: Path, view_fqn: str) -> ViewCatalog | None:
    data = _load_catalog_file(project_root, "views", view_fqn)
    return ViewCatalog.model_validate(data) if data is not None else None


def load_function_catalog(project_root: Path, func_fqn: str) -> FunctionCatalog | None:
    data = _load_catalog_file(project_root, "functions", func_fqn)
    return FunctionCatalog.model_validate(data) if data is not None else None


def read_selected_writer(project_root: Path, table_fqn: str) -> str | None:
    """Read selected_writer from the scoping section of a table catalog file.

    Returns None if the table catalog doesn't exist or has no scoping section
    or scoping.selected_writer is not set.
    """
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        return None
    if cat.scoping is None:
        return None
    return cat.scoping.selected_writer


# ── Writing ─────────────────────────────────────────────────────────────────


def write_json(path: Path, data: dict[str, Any]) -> None:
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


def resolve_catalog_path(project_root: Path, fqn: str) -> Path:
    """Resolve the catalog JSON path for a table or view FQN.

    Checks views first, then tables. Raises CatalogFileMissingError if neither exists.
    """
    catalog_dir = resolve_catalog_dir(project_root)
    view_path = catalog_dir / "views" / f"{fqn}.json"
    if view_path.exists():
        return view_path
    table_path = catalog_dir / "tables" / f"{fqn}.json"
    if table_path.exists():
        return table_path
    raise CatalogFileMissingError("table or view", fqn)


def detect_catalog_bucket(project_root: Path, fqn: str) -> str | None:
    """Return ``tables`` or ``views`` if a catalog file exists for the FQN."""
    norm = normalize(fqn)
    catalog_dir = resolve_catalog_dir(project_root)
    if (catalog_dir / "tables" / f"{norm}.json").exists():
        return "tables"
    if (catalog_dir / "views" / f"{norm}.json").exists():
        return "views"
    return None


def detect_object_type(project_root: Path, fqn: str) -> str | None:
    """Detect whether a normalized FQN refers to a table, view, or MV."""
    norm = normalize(fqn)
    bucket = detect_catalog_bucket(project_root, norm)
    if bucket == "tables":
        return "table"
    if bucket == "views":
        try:
            cat = load_view_catalog(project_root, norm)
            if cat and cat.is_materialized_view:
                return "mv"
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            pass
        return "view"
    return None


def load_and_merge_catalog(
    project_root: Path,
    fqn: str,
    section_key: str,
    section_data: Any,
) -> dict[str, Any]:
    """Load a catalog file, merge a section into it, and write it back atomically.

    Auto-detects table vs view by checking catalog/views/ first, then catalog/tables/.
    Returns a confirmation dict with ``ok``, ``table``, ``status``, and ``catalog_path``.

    Raises CatalogFileMissingError if no catalog file exists for the FQN.
    Raises CatalogLoadError on corrupt JSON.
    """
    cat_path = resolve_catalog_path(project_root, fqn)

    try:
        existing = json.loads(cat_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(cat_path), exc) from exc

    existing[section_key] = section_data
    write_json(cat_path, existing)

    result: dict[str, Any] = {
        "ok": True,
        "table": fqn,
        "catalog_path": str(cat_path),
    }
    if isinstance(section_data, dict) and "status" in section_data:
        result["status"] = section_data["status"]
    return result


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
    *,
    ddl_hash: str | None = None,
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
        "excluded": False,
        "is_source": False,
    }
    data: dict[str, Any] = {"schema": schema, "name": name, **defaults, **signals}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if referenced_by is not None:
        data["referenced_by"] = referenced_by
    else:
        data.setdefault("referenced_by", {
            "procedures": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
        })
    p = _object_path(project_root, "tables", fqn)
    write_json(p, data)
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
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(p), exc) from exc
    data["statements"] = statements
    llm_recovered = any(stmt.get("source") == "llm" for stmt in statements if isinstance(stmt, dict))
    if llm_recovered:
        existing_errors = data.get("errors")
        if isinstance(existing_errors, list):
            filtered_errors = [
                err
                for err in existing_errors
                if not (isinstance(err, dict) and err.get("code") == "PARSE_ERROR")
            ]
            if len(filtered_errors) != len(existing_errors):
                logger.info(
                    "event=proc_parse_error_cleared component=catalog operation=write_proc_statements proc=%s status=success",
                    norm,
                )
                data["errors"] = filtered_errors
    write_json(p, data)
    return p


def write_proc_table_slice(
    project_root: Path, proc_fqn: str, table_fqn: str, ddl_slice: str
) -> Path:
    """Upsert a per-table DDL slice into the proc catalog table_slices section."""
    norm_proc = normalize(proc_fqn)
    norm_table = normalize(table_fqn)
    cat_path = _object_path(project_root, "procedures", norm_proc)
    if not cat_path.exists():
        raise CatalogFileMissingError("procedure", norm_proc)
    try:
        data = json.loads(cat_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(cat_path), exc) from exc
    data.setdefault("table_slices", {})[norm_table] = ddl_slice
    write_json(cat_path, data)
    return cat_path


def _write_catalog_json(
    project_root: Path, object_type: str, norm_fqn: str, data: dict[str, Any]
) -> Path:
    p = _object_path(project_root, object_type, norm_fqn)
    write_json(p, data)
    return p


def write_proc_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    needs_llm: bool = False,
    needs_enrich: bool = False,
    mode: str | None = None,
    routing_reasons: list[str] | None = None,
    params: list[dict[str, Any]] | None = None,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a procedure catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
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
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    return _write_catalog_json(project_root, "procedures", norm, data)


def write_view_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    sql: str | None = None,
    columns: list[dict[str, Any]] | None = None,
    is_materialized_view: bool = False,
    long_truncation: bool = False,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a view catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references, "excluded": False}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if is_materialized_view:
        data["is_materialized_view"] = True
    if sql is not None:
        data["sql"] = sql
    if columns is not None:
        data["columns"] = columns
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    if long_truncation:
        data["long_truncation"] = True
    return _write_catalog_json(project_root, "views", norm, data)


def write_function_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    subtype: str | None = None,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a function catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if subtype is not None:
        data["subtype"] = subtype
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    return _write_catalog_json(project_root, "functions", norm, data)


# ── Enrichment field preservation (re-extraction merge) ─────────────────────

# Keys preserved per bucket during re-extraction. ``refactor`` belongs only on
# procedure catalogs; never copy it from tables/views/functions.
_ENRICHED_KEYS_BY_BUCKET: dict[str, tuple[str, ...]] = {
    "tables": ("scoping", "profile", "excluded", "is_source", "is_seed"),
    "procedures": ("scoping", "profile", "refactor"),
    "views": ("scoping", "profile", "excluded"),
    "functions": ("scoping", "profile"),
}


def snapshot_enriched_fields(project_root: Path) -> dict[str, dict[str, Any]]:
    """Snapshot LLM-enriched fields from all existing catalog files.

    Returns a mapping of normalised FQN → dict containing only the
    non-None enriched keys for that bucket.
    ``refactor`` is only captured from procedure catalogs.
    Used before re-extraction so these fields survive a catalog rewrite.
    """
    catalog_dir = _catalog_dir(project_root)
    snapshot: dict[str, dict[str, Any]] = {}
    if not catalog_dir.is_dir():
        return snapshot
    for bucket, keys in _ENRICHED_KEYS_BY_BUCKET.items():
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_file in bucket_dir.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            enriched = {k: data[k] for k in keys if data.get(k) is not None}
            if enriched:
                snapshot[json_file.stem] = enriched
    return snapshot


def restore_enriched_fields(
    project_root: Path, snapshot: dict[str, dict[str, Any]]
) -> None:
    """Restore LLM-enriched fields into catalog files after re-extraction.

    For each FQN in *snapshot*, reads the catalog file (if present), merges
    the snapshotted enriched fields back in, and writes the file.  Files not
    touched by re-extraction are not written again.
    """
    catalog_dir = _catalog_dir(project_root)
    for fqn, enriched in snapshot.items():
        for bucket in ("tables", "procedures", "views", "functions"):
            p = catalog_dir / bucket / f"{fqn}.json"
            if not p.exists():
                continue
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            changed = False
            for key, value in enriched.items():
                if data.get(key) != value:
                    data[key] = value
                    changed = True
            if changed:
                write_json(p, data)
            break  # found the bucket — no need to check others
        else:
            logger.debug("event=catalog_restore_skip fqn=%s reason=not_found_after_reextract", fqn)
