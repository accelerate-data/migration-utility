"""Directory loading, catalog indexing, and on-disk I/O for DDL files."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from shared.loader_data import (
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
)
from shared.loader_parse import (
    _GO_RE,
    _extract_name,
    _extract_type_bucket,
    _parse_block,
    _split_blocks,
    extract_refs,
)
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")

_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": _GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}

# catalog.json format version
_CATALOG_SCHEMA_VERSION = "1.0"


# ── Manifest ─────────────────────────────────────────────────────────────────


def read_manifest(ddl_path: Path) -> dict[str, str]:
    """Read manifest.json from ddl_path if present. Returns dialect, defaulting to tsql."""
    manifest_file = Path(ddl_path) / "manifest.json"
    if manifest_file.exists():
        try:
            with manifest_file.open() as f:
                m = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {ddl_path} is not valid JSON: {exc}") from exc
        return {"dialect": m.get("dialect", "tsql")}
    return {"dialect": "tsql"}


# ── Directory loading ────────────────────────────────────────────────────────


def _load_file(
    path: Path,
    catalog: DdlCatalog,
    dialect: str = "tsql",
    delimiter_re: re.Pattern[str] = _GO_RE,
) -> None:
    """Parse a .sql file and route each block into the correct catalog bucket."""
    if not path.exists():
        return
    blocks = _split_blocks(path.read_text(encoding="utf-8"), delimiter_re=delimiter_re)
    for block in blocks:
        raw_name = _extract_name(block)
        if not raw_name:
            logger.warning("event=load_file operation=extract_name file=%s reason=no_name_found", path.name)
            continue
        bucket_name = _extract_type_bucket(block)
        if not bucket_name:
            logger.warning("event=load_file operation=extract_type file=%s object=%s reason=unknown_type", path.name, raw_name)
            continue
        key = normalize(raw_name)
        try:
            ast = _parse_block(block, dialect=dialect)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=ast)
        except DdlParseError as exc:
            logger.warning("event=load_file operation=parse_block file=%s object=%s error=%s", path.name, raw_name, exc)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=None, parse_error=str(exc))


def load_directory(ddl_path: Path | str, dialect: str = "tsql") -> DdlCatalog:
    """Read all .sql files in a DDL directory and return a populated DdlCatalog.

    Object types are auto-detected from CREATE statements — filenames are not
    significant.  Any .sql file may contain any mix of tables, procedures,
    views, and functions separated by GO delimiters.

    If a manifest.json is present in ddl_path, the dialect declared there
    overrides the dialect parameter.

    Args:
        ddl_path: Path to directory containing .sql files.
        dialect:  sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    path = Path(ddl_path)
    if not path.exists():
        raise FileNotFoundError(f"DDL path does not exist: {path}")

    ddl_dir = path / "ddl"
    if not ddl_dir.is_dir():
        raise FileNotFoundError(f"ddl/ subdirectory does not exist: {ddl_dir}")

    _manifest = read_manifest(path)
    dialect = _manifest["dialect"]
    delimiter_re = _DELIMITER_MAP.get(dialect, _GO_RE)

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
    ddl_path: Path | str,
    output_dir: Path | str,
    dialect: str = "tsql",
) -> None:
    """Split a DDL directory into per-object files and write a catalog.json.

    Creates per-type subdirectories with individual .sql files and a
    ``catalog.json`` reference graph index.

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    _manifest = read_manifest(Path(ddl_path))
    dialect = _manifest["dialect"]
    catalog = load_directory(ddl_path, dialect=dialect)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    objects = _write_per_object_files(catalog, out)

    catalog_doc = {
        "schema_version": _CATALOG_SCHEMA_VERSION,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "source": str(Path(ddl_path).resolve()),
        "objects": objects,
    }
    (out / "catalog.json").write_text(
        json.dumps(catalog_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_ddl(ddl_path: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a DDL directory.

    Requires a catalog/ directory (from setup-ddl). Raises CatalogNotFoundError
    if missing. Prefers pre-built catalog.json when available, falls back to
    parsing .sql files directly.
    """
    manifest = read_manifest(ddl_path)
    dialect = manifest["dialect"]
    catalog_dir = ddl_path / "catalog"
    if not catalog_dir.is_dir() or not any(catalog_dir.rglob("*.json")):
        logger.error("event=load_ddl operation=check_catalog ddl_path=%s reason=no_catalog_directory", ddl_path)
        raise CatalogNotFoundError(ddl_path)
    catalog_json = ddl_path / "catalog.json"
    if catalog_json.exists():
        return load_catalog(ddl_path), dialect
    return load_directory(ddl_path, dialect=dialect), dialect


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
