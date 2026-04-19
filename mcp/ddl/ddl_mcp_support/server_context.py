"""Runtime context and parsing helpers for the DDL MCP server."""

from __future__ import annotations

import os
from pathlib import Path

import sqlglot.expressions as exp

from ddl_mcp_support.env_config import assert_git_repo
from ddl_mcp_support.loader import DdlCatalog, DdlEntry, load_directory, read_manifest


class DdlServerContext:
    """Resolve and cache the project catalog used by tool handlers."""

    def __init__(self) -> None:
        self._project_root_cache: dict[tuple[str, str], Path] = {}
        self._catalog_cache: dict[Path, DdlCatalog] = {}
        self._catalog_dialect_cache: dict[Path, str] = {}
        self._catalog_token_cache: dict[Path, tuple[tuple[str, int], ...]] = {}

    def project_root(self) -> Path:
        cache_key = (os.environ.get("DDL_PATH", "").strip(), str(Path.cwd()))
        cached = self._project_root_cache.get(cache_key)
        if cached is not None:
            return cached

        raw = os.environ.get("DDL_PATH", "").strip()
        project_root = Path(raw) if raw else Path.cwd()
        if not project_root.exists():
            raise FileNotFoundError(f"Project root does not exist: {project_root}")
        assert_git_repo(project_root)
        manifest = project_root / "manifest.json"
        if not manifest.exists():
            raise FileNotFoundError(
                f"manifest.json not found in {project_root}. "
                "Run setup-ddl first or set DDL_PATH."
            )
        self._project_root_cache[cache_key] = project_root
        return project_root

    def catalog(self) -> DdlCatalog:
        project_root = self.project_root()
        resolved = project_root.resolve()
        token = ddl_cache_token(project_root)
        dialect = self._catalog_dialect_cache.get(resolved)
        if dialect is None:
            manifest = read_manifest(project_root)
            dialect = manifest["dialect"]
        if self._catalog_token_cache.get(resolved) != token or resolved not in self._catalog_cache:
            self._catalog_cache[resolved] = load_directory(project_root, dialect=dialect)
            self._catalog_token_cache[resolved] = token
        self._catalog_dialect_cache[resolved] = dialect
        return self._catalog_cache[resolved]

    def catalog_dialect(self) -> str:
        project_root = self.project_root()
        resolved = project_root.resolve()
        if resolved not in self._catalog_dialect_cache:
            self.catalog()
        return self._catalog_dialect_cache[resolved]

    def clear_caches(self) -> None:
        self._project_root_cache.clear()
        self._catalog_cache.clear()
        self._catalog_dialect_cache.clear()
        self._catalog_token_cache.clear()


def ddl_cache_token(project_root: Path) -> tuple[tuple[str, int], ...]:
    manifest = project_root / "manifest.json"
    tracked_paths = [manifest, *sorted(project_root.rglob("*.sql"))]
    return tuple(
        (str(path.relative_to(project_root)), path.stat().st_mtime_ns)
        for path in tracked_paths
        if path.exists()
    )


def require_argument(arguments: dict, key: str) -> str | None:
    value = arguments.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def parse_columns(entry: DdlEntry, dialect: str = "tsql") -> list[dict]:
    """Parse column metadata from a CREATE TABLE AST entry."""
    if entry.ast is None:
        return []
    columns = []
    for column_definition in entry.ast.find_all(exp.ColumnDef):
        is_pk = False
        is_not_null = False
        for constraint in column_definition.constraints:
            kind = constraint.kind
            if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                is_pk = True
            elif isinstance(kind, exp.NotNullColumnConstraint):
                if not kind.args.get("allow_null", False):
                    is_not_null = True
        columns.append({
            "name": column_definition.name,
            "type": (
                column_definition.kind.sql(dialect=dialect)
                if column_definition.kind
                else "UNKNOWN"
            ),
            "nullable": not (is_not_null or is_pk),
            "is_pk": is_pk,
        })
    return columns
