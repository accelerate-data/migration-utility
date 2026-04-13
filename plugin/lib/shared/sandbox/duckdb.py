"""DuckDB sandbox backend."""

from __future__ import annotations

import logging
import re
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator

from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.runtime_config import get_runtime_role
from shared.sandbox.base import (
    SandboxBackend,
    build_compare_error,
    build_compare_result,
    build_execute_error,
    build_execute_output,
    serialize_rows,
    validate_fixture_rows,
    validate_readonly_sql as _validate_readonly_sql_base,
)

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)

_duckdb = None


def _import_duckdb():
    global _duckdb
    if _duckdb is None:
        try:
            import duckdb
        except ImportError as exc:
            raise ImportError(
                "duckdb is required for DuckDB sandbox connectivity. "
                "Install it with: uv pip install duckdb"
            ) from exc
        _duckdb = duckdb
    return _duckdb


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|COPY|CALL)\b",
    re.IGNORECASE,
)


def _normalize_identifier(name: str) -> str:
    if not name:
        raise ValueError(f"Unsafe DuckDB identifier: {name!r}")
    if re.search(r"[;'\"\\]", name):
        raise ValueError(f"Unsafe DuckDB identifier: {name!r}")
    cleaned = name.replace("[", "").replace("]", "")
    parts = [part.strip() for part in cleaned.split(".") if part.strip()]
    if not parts:
        raise ValueError(f"Unsafe DuckDB identifier: {name!r}")
    if len(parts) > 2:
        parts = parts[-2:]
    for part in parts:
        if not _IDENTIFIER_RE.match(part):
            raise ValueError(f"Unsafe DuckDB identifier: {name!r}")
    return ".".join(f'"{part}"' for part in parts)


def _sandbox_path_from_name(name: str) -> Path:
    path = Path(name)
    if path.suffix != ".duckdb":
        raise ValueError(f"Invalid DuckDB sandbox path: {name!r}")
    return path


def _capture_rows(cursor: Any) -> list[dict[str, Any]]:
    description = cursor.description
    if not description:
        return []
    columns = [col[0] for col in description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class DuckDbSandbox(SandboxBackend):
    """Manage a throwaway DuckDB database file for local execution."""

    def __init__(self, source_path: str, sandbox_path: str | None = None) -> None:
        self.source_path = source_path
        self.sandbox_path = sandbox_path

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> DuckDbSandbox:
        source_role = get_runtime_role(manifest, "source")
        sandbox_role = get_runtime_role(manifest, "sandbox")
        if source_role is None or not source_role.connection.path:
            raise ValueError("manifest.json is missing runtime.source.connection.path for DuckDB")
        if sandbox_role is None or not sandbox_role.connection.path:
            raise ValueError("manifest.json is missing runtime.sandbox.connection.path for DuckDB")
        return cls(
            source_path=source_role.connection.path,
            sandbox_path=sandbox_role.connection.path,
        )

    @contextmanager
    def _connect(self, database_path: str) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        conn = _import_duckdb().connect(database=str(database_path))
        try:
            yield conn
        finally:
            conn.close()

    def _resolve_sandbox_path(self, sandbox_db: str | None = None) -> Path:
        if sandbox_db:
            return _sandbox_path_from_name(sandbox_db)
        if self.sandbox_path:
            return Path(self.sandbox_path)
        source_path = Path(self.source_path)
        return source_path.with_name(f"__test_{source_path.stem}.duckdb")

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        sandbox_path = self._resolve_sandbox_path()
        source_path = Path(self.source_path)
        sandbox_path.parent.mkdir(parents=True, exist_ok=True)

        if not source_path.exists():
            return SandboxUpOutput(
                sandbox_database=str(sandbox_path),
                status="error",
                tables_cloned=[],
                views_cloned=[],
                procedures_cloned=[],
                errors=[ErrorEntry(code="SOURCE_NOT_FOUND", message=f"Source DuckDB file not found: {source_path}")],
            )

        shutil.copy2(source_path, sandbox_path)
        with self._connect(str(sandbox_path)) as conn:
            placeholders = ", ".join("?" for _ in schemas)
            rows = conn.execute(
                (
                    "select table_schema, table_name "
                    "from information_schema.tables "
                    f"where table_schema in ({placeholders}) and table_type = 'BASE TABLE' "
                    "order by table_schema, table_name"
                ),
                schemas,
            ).fetchall()
        logger.info("event=duckdb_sandbox_up sandbox=%s source=%s", sandbox_path, source_path)
        return SandboxUpOutput(
            sandbox_database=str(sandbox_path),
            status="ok",
            tables_cloned=[f"{schema}.{table}" for schema, table in rows],
            views_cloned=[],
            procedures_cloned=[],
            errors=[],
        )

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        sandbox_path = _sandbox_path_from_name(sandbox_db)
        try:
            sandbox_path.unlink(missing_ok=True)
            logger.info("event=duckdb_sandbox_down sandbox=%s", sandbox_path)
            return SandboxDownOutput(sandbox_database=str(sandbox_path), status="ok")
        except OSError as exc:
            return SandboxDownOutput(
                sandbox_database=str(sandbox_path),
                status="error",
                errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message=str(exc))],
            )

    def sandbox_status(self, sandbox_db: str) -> SandboxStatusOutput:
        sandbox_path = _sandbox_path_from_name(sandbox_db)
        exists = sandbox_path.exists()
        return SandboxStatusOutput(
            sandbox_database=str(sandbox_path),
            status="ok" if exists else "not_found",
            exists=exists,
            errors=[],
        )

    def _seed_fixtures(self, cursor: Any, fixtures: list[dict[str, Any]]) -> None:
        for fixture in fixtures:
            table = _normalize_identifier(fixture["table"])
            rows = fixture.get("rows", [])
            if not rows:
                continue
            validate_fixture_rows(fixture["table"], rows)
            columns = list(rows[0].keys())
            for column in columns:
                _normalize_identifier(column)
            placeholders = ", ".join("?" for _ in columns)
            column_sql = ", ".join(f'"{column}"' for column in columns)
            values = [[row.get(column) for column in columns] for row in rows]
            cursor.executemany(
                f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
                values,
            )

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        raise NotImplementedError(
            "DuckDB sandbox does not support procedure-based execute_scenario flows. "
            "Use execute_select-based scenarios or a backend with stored procedure execution support."
        )

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        _validate_readonly_sql_base(sql, _WRITE_SQL_RE)
        sandbox_path = _sandbox_path_from_name(sandbox_db)
        try:
            with self._connect(str(sandbox_path)) as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    self._seed_fixtures(conn, fixtures)
                    cursor = conn.execute(sql)
                    rows = serialize_rows(_capture_rows(cursor))
                finally:
                    conn.execute("ROLLBACK")
        except Exception as exc:  # duckdb raises multiple subclasses
            return build_execute_error("execute_select", "SCENARIO_FAILED", str(exc))

        return build_execute_output("execute_select", rows)

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        _validate_readonly_sql_base(sql_a, _WRITE_SQL_RE)
        _validate_readonly_sql_base(sql_b, _WRITE_SQL_RE)
        sandbox_path = _sandbox_path_from_name(sandbox_db)
        try:
            with self._connect(str(sandbox_path)) as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    self._seed_fixtures(conn, fixtures)
                    rows_a = serialize_rows(_capture_rows(conn.execute(sql_a)))
                    rows_b = serialize_rows(_capture_rows(conn.execute(sql_b)))
                finally:
                    conn.execute("ROLLBACK")
        except Exception as exc:
            return build_compare_error("COMPARE_FAILED", str(exc))

        return build_compare_result(rows_a, rows_b)
