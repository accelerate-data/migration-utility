"""Directory loading and manifest I/O for standalone DDL MCP support."""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import sqlglot.expressions as exp

from ddl_mcp_support.loader_data import DdlCatalog, DdlEntry, DdlParseError
from ddl_mcp_support.loader_parse import (
    GO_RE,
    extract_name,
    extract_type_bucket,
    parse_block,
    split_blocks,
)
from ddl_mcp_support.name_resolver import normalize

logger = logging.getLogger(__name__)


_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")

_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}


def read_manifest(project_root: Path) -> dict[str, Any]:
    manifest_file = Path(project_root) / "manifest.json"
    if manifest_file.exists():
        try:
            with manifest_file.open(encoding="utf-8") as f:
                m = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {project_root} is not valid JSON: {exc}") from exc
        m["dialect"] = m.get("dialect", "tsql")
        return m
    return {"dialect": "tsql"}


def _resolve_ddl_dir(project_root: Path) -> Path:
    raw = os.environ.get("DDL_DIR", "").strip()
    return Path(raw) if raw else project_root / "ddl"


def _load_file(
    path: Path,
    catalog: DdlCatalog,
    dialect: str = "tsql",
    delimiter_re: re.Pattern[str] = GO_RE,
) -> None:
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
            logger.warning(
                "event=load_file operation=extract_type file=%s object=%s reason=unknown_type",
                path.name,
                raw_name,
            )
            continue
        key = normalize(raw_name)
        try:
            ast = parse_block(block, dialect=dialect)
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
    path = Path(project_root)
    if not path.exists():
        raise FileNotFoundError(f"Project root does not exist: {path}")

    ddl_dir = _resolve_ddl_dir(path)
    if not ddl_dir.is_dir():
        raise FileNotFoundError(f"ddl/ subdirectory does not exist: {ddl_dir}")

    manifest = read_manifest(path)
    dialect = manifest["dialect"]
    delimiter_re = _DELIMITER_MAP.get(dialect, GO_RE)

    catalog = DdlCatalog()
    for sql_file in sorted(ddl_dir.glob("*.sql")):
        _load_file(sql_file, catalog, dialect=dialect, delimiter_re=delimiter_re)
    return catalog
