"""Directory loading helpers for shared.loader_io."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from shared.env_config import resolve_ddl_dir
from shared.loader_data import CatalogNotFoundError, DdlCatalog, DdlEntry, DdlParseError
from shared.loader_io_support.manifest import read_manifest
from shared.loader_parse import (
    GO_RE,
    extract_name,
    extract_type_bucket,
    parse_block,
    split_blocks,
)
from shared.env_config import resolve_catalog_dir
from shared.name_resolver import normalize
from sqlglot import exp

logger = logging.getLogger(__name__)

_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")

_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}


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
            # Detect nested Command nodes (unsupported syntax within valid AST)
            unsupported: list[str] = []
            for node in ast.walk():
                if isinstance(node, exp.Command):
                    unsupported.append(str(node)[:200])
            getattr(catalog, bucket_name)[key] = DdlEntry(
                raw_ddl=block,
                ast=ast,
                unsupported_syntax_nodes=unsupported if unsupported else None,
            )
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


def load_ddl(project_root: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a project root directory.

    Requires a ``catalog/`` directory (from setup-ddl) containing per-object
    JSON files. Raises ``CatalogNotFoundError`` if the directory is missing or
    empty.

    Two loading strategies:

    1. **Pre-built index** — if ``catalog.json`` exists (written by
       ``index_directory``), loads from that flat reference index.  This is
       faster but does not include sqlglot AST data.
    2. **Live parse** — parses ``.sql`` DDL files directly via sqlglot.

    The ``catalog/`` directory guard ensures we don't silently use a stale
    ``catalog.json`` from a different project state.
    """

    manifest = read_manifest(project_root)
    dialect = manifest.get("dialect", "tsql")
    catalog_dir = resolve_catalog_dir(project_root)
    if not catalog_dir.is_dir() or not any(catalog_dir.rglob("*.json")):
        logger.error("event=load_ddl operation=check_catalog project_root=%s reason=no_catalog_directory", project_root)
        raise CatalogNotFoundError(project_root)
    catalog_json = project_root / "catalog.json"
    if catalog_json.exists():
        from shared.loader_io_support.indexing import load_catalog

        return load_catalog(project_root), dialect
    return load_directory(project_root, dialect=dialect), dialect
