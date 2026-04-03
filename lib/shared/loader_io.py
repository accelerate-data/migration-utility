"""Directory loading, catalog indexing, and on-disk I/O for DDL files."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.loader_data import (
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
)
from shared.loader_parse import (
    GO_RE,
    extract_name,
    extract_refs,
    extract_type_bucket,
    parse_block,
    split_blocks,
)
from shared.env_config import resolve_catalog_dir, resolve_ddl_dir
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")

_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}

# catalog.json format version
_CATALOG_SCHEMA_VERSION = "1.0"


# ── Manifest ─────────────────────────────────────────────────────────────────


def read_manifest(project_root: Path) -> dict[str, Any]:
    """Read manifest.json from project_root if present.

    Returns the full manifest dict with dialect defaulting to tsql.
    If manifest.json does not exist, returns a minimal dict with only dialect.
    """
    manifest_file = Path(project_root) / "manifest.json"
    if manifest_file.exists():
        try:
            with manifest_file.open() as f:
                m = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {project_root} is not valid JSON: {exc}") from exc
        m.setdefault("dialect", "tsql")
        return m
    return {"dialect": "tsql"}


def _require_manifest_file(project_root: Path) -> Path:
    """Return manifest.json path, raising if it does not exist on disk."""
    manifest_file = Path(project_root) / "manifest.json"
    if not manifest_file.exists():
        raise FileNotFoundError(f"manifest.json not found in {project_root}")
    return manifest_file


def write_manifest_sandbox(project_root: Path, run_id: str, database: str) -> None:
    """Persist sandbox run_id and database name into manifest.json."""
    manifest_file = _require_manifest_file(project_root)
    manifest = read_manifest(project_root)
    manifest["sandbox"] = {"run_id": run_id, "database": database}
    manifest_file.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "event=manifest_update operation=write_sandbox run_id=%s database=%s",
        run_id,
        database,
    )


def clear_manifest_sandbox(project_root: Path) -> None:
    """Remove the sandbox key from manifest.json."""
    manifest_file = _require_manifest_file(project_root)
    manifest = read_manifest(project_root)
    manifest.pop("sandbox", None)
    manifest_file.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("event=manifest_update operation=clear_sandbox")


# ── Directory loading ────────────────────────────────────────────────────────


def _load_file(
    path: Path,
    catalog: DdlCatalog,
    dialect: str = "tsql",
    delimiter_re: re.Pattern[str] = GO_RE,
) -> None:
    """Parse a .sql file and route each block into the correct catalog bucket."""
    if not path.exists():
        return
    blocks = split_blocks(path.read_text(encoding="utf-8"), delimiter_re=delimiter_re)
    for block in blocks:
        raw_name = extract_name(block)
        if not raw_name:
            logger.warning("event=load_file operation=extract_name file=%s reason=no_name_found", path.name)
            continue
        bucket_name = extract_type_bucket(block)
        if not bucket_name:
            logger.warning("event=load_file operation=extract_type file=%s object=%s reason=unknown_type", path.name, raw_name)
            continue
        key = normalize(raw_name)
        try:
            ast = parse_block(block, dialect=dialect)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=ast)
        except DdlParseError as exc:
            logger.warning("event=load_file operation=parse_block file=%s object=%s error=%s", path.name, raw_name, exc)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=None, parse_error=str(exc))


def load_directory(project_root: Path | str, dialect: str = "tsql") -> DdlCatalog:
    """Read all .sql files in a DDL directory and return a populated DdlCatalog.

    Object types are auto-detected from CREATE statements — filenames are not
    significant.  Any .sql file may contain any mix of tables, procedures,
    views, and functions separated by GO delimiters.

    If a manifest.json is present in project_root, the dialect declared there
    overrides the dialect parameter.

    Args:
        project_root: Path to project root directory containing ddl/ and manifest.json.
        dialect:  sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if project_root does not exist.
    """
    path = Path(project_root)
    if not path.exists():
        raise FileNotFoundError(f"Project root does not exist: {path}")

    ddl_dir = resolve_ddl_dir(path)
    if not ddl_dir.is_dir():
        raise FileNotFoundError(f"ddl/ subdirectory does not exist: {ddl_dir}")

    _manifest = read_manifest(path)
    dialect = _manifest["dialect"]
    delimiter_re = _DELIMITER_MAP.get(dialect, GO_RE)

    catalog = DdlCatalog()
    for sql_file in sorted(ddl_dir.glob("*.sql")):
        _load_file(sql_file, catalog, dialect=dialect, delimiter_re=delimiter_re)
    return catalog


# ── On-disk index ────────────────────────────────────────────────────────────


def _write_per_object_files(
    catalog: DdlCatalog, out: Path,
) -> dict[str, dict]:
    """Write per-object .sql files and extract refs for catalog.json.

    Returns ``{name: {type, file, writes_to, reads_from, calls, parse_error}}``.
    """
    bucket_map = {
        "tables": catalog.tables,
        "procedures": catalog.procedures,
        "views": catalog.views,
        "functions": catalog.functions,
    }

    objects: dict[str, dict] = {}
    for bucket, entries in bucket_map.items():
        if not entries:
            continue
        bucket_dir = out / bucket
        bucket_dir.mkdir(exist_ok=True)
        for name, entry in entries.items():
            file_name = f"{name}.sql"
            (bucket_dir / file_name).write_text(entry.raw_ddl, encoding="utf-8")

            type_label = bucket.rstrip("s")
            refs_dict: dict = {"writes_to": [], "reads_from": [], "calls": []}
            parse_error: str | None = entry.parse_error
            if parse_error is None:
                try:
                    refs = extract_refs(entry)
                    refs_dict = {
                        "writes_to": refs.writes_to,
                        "reads_from": refs.reads_from,
                        "calls": refs.calls,
                    }
                except DdlParseError as exc:
                    parse_error = str(exc)

            objects[name] = {
                "type": type_label,
                "file": f"{bucket}/{file_name}",
                **refs_dict,
                "parse_error": parse_error,
            }
    return objects


def index_directory(
    project_root: Path | str,
    output_dir: Path | str,
    dialect: str = "tsql",
) -> None:
    """Split a DDL directory into per-object files and write a catalog.json.

    Creates per-type subdirectories with individual .sql files and a
    ``catalog.json`` reference graph index.

    Raises:
        FileNotFoundError: if project_root does not exist.
    """
    _manifest = read_manifest(Path(project_root))
    dialect = _manifest["dialect"]
    catalog = load_directory(project_root, dialect=dialect)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    objects = _write_per_object_files(catalog, out)

    catalog_doc = {
        "schema_version": _CATALOG_SCHEMA_VERSION,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "source": str(Path(project_root).resolve()),
        "objects": objects,
    }
    (out / "catalog.json").write_text(
        json.dumps(catalog_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_ddl(project_root: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a project root directory.

    Requires a catalog/ directory (from setup-ddl). Raises CatalogNotFoundError
    if missing. Prefers pre-built catalog.json when available, falls back to
    parsing .sql files directly.
    """
    manifest = read_manifest(project_root)
    dialect = manifest["dialect"]
    catalog_dir = resolve_catalog_dir(project_root)
    if not catalog_dir.is_dir() or not any(catalog_dir.rglob("*.json")):
        logger.error("event=load_ddl operation=check_catalog project_root=%s reason=no_catalog_directory", project_root)
        raise CatalogNotFoundError(project_root)
    catalog_json = project_root / "catalog.json"
    if catalog_json.exists():
        return load_catalog(project_root), dialect
    return load_directory(project_root, dialect=dialect), dialect


def load_catalog(output_dir: Path | str) -> DdlCatalog:
    """Load a DdlCatalog from a pre-built index directory.

    Reads catalog.json and the individual .sql files. Does not re-parse with
    sqlglot — DdlEntry.ast is None for all entries.

    Args:
        output_dir: Directory previously written by index_directory().

    Raises:
        FileNotFoundError: if output_dir or catalog.json does not exist.
    """
    out = Path(output_dir)
    catalog_path = out / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found in {out}")

    doc = json.loads(catalog_path.read_text(encoding="utf-8"))
    objects = doc.get("objects", {})

    result = DdlCatalog()
    bucket_attr = {
        "table": "tables",
        "procedure": "procedures",
        "view": "views",
        "function": "functions",
    }

    for name, obj in objects.items():
        file_path = out / obj["file"]
        raw_ddl = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
        entry = DdlEntry(
            raw_ddl=raw_ddl,
            ast=None,
            parse_error=obj.get("parse_error"),
        )
        attr = bucket_attr.get(obj.get("type", ""), "")
        if attr:
            getattr(result, attr)[name] = entry

    return result
