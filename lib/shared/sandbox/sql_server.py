"""SQL Server sandbox backend using pyodbc."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyodbc

from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.db_connect import build_sql_server_connection_string
from shared.sandbox.base import (
    SandboxBackend,
    build_compare_error,
    build_compare_result,
    build_execute_error,
    build_execute_output,
    capture_rows as _capture_rows_base,
    generate_sandbox_name,
    validate_fixtures as _validate_fixtures_base,
    validate_readonly_sql as _validate_readonly_sql_base,
)
from shared.runtime_config import get_runtime_role

logger = logging.getLogger(__name__)


_pyodbc = None


def _import_pyodbc():
    """Lazy-import pyodbc so the module can be imported without it installed."""
    global _pyodbc
    if _pyodbc is None:
        try:
            import pyodbc
        except ImportError as exc:
            raise ImportError(
                "pyodbc is required for SQL Server connectivity. "
                "Install it with: uv pip install pyodbc"
            ) from exc
        _pyodbc = pyodbc
    return _pyodbc

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_ ]*$")
# Bracket-quoted identifiers may additionally contain hyphens
_BRACKETED_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_ -]*$")
_REMOTE_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+"
    r"(?:@\w+\s*=\s*)?"
    r"(?!sp_executesql\b)(?![@(])"
    r"(?P<target>"
    r"(?:\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_ ]*)"
    r"(?:\.(?:\[[^\]]+\]|[a-zA-Z_][a-zA-Z0-9_ ]*)){2,3}"
    r")",
    re.IGNORECASE,
)


def _validate_identifier(name: str) -> None:
    """Validate a SQL identifier (schema, table, procedure name) is safe.

    Accepts bracket-quoted identifiers like [dbo].[Product] or plain
    identifiers like dbo.Product. Rejects anything containing quotes,
    semicolons, backslashes, or other injection vectors.
    """
    if not name:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    if re.search(r"[;'\"\\]", name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    bracket_stripped = re.sub(r"\[([^\[\]]+)\]", "", name)
    if "[" in bracket_stripped or "]" in bracket_stripped:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    segments = re.findall(r"\[([^\[\]]+)\]|([^.\[\]]+)", name)
    if not segments:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    for bracketed, bare in segments:
        if bracketed:
            if not _BRACKETED_IDENTIFIER_RE.match(bracketed):
                raise ValueError(f"Unsafe SQL identifier: {name!r}")
        elif bare:
            if not _IDENTIFIER_RE.match(bare):
                raise ValueError(f"Unsafe SQL identifier: {name!r}")
        else:
            raise ValueError(f"Unsafe SQL identifier: {name!r}")


def _validate_sandbox_db_name(sandbox_db: str) -> None:
    """Validate a sandbox database name is safe for interpolation.

    Sandbox names follow the pattern __test_<hex> and must only contain
    safe characters.
    """
    if not re.match(r"^__test_[a-zA-Z0-9_]{1,128}$", sandbox_db):
        raise ValueError(f"Invalid sandbox database name: {sandbox_db!r}")

def _split_identifier_parts(identifier: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_brackets = False

    for char in identifier:
        if char == "[":
            in_brackets = True
        elif char == "]":
            in_brackets = False
        elif char == "." and not in_brackets:
            part = "".join(current).strip()
            if part:
                parts.append(part.strip("[]"))
            current = []
            continue
        current.append(char)

    part = "".join(current).strip()
    if part:
        parts.append(part.strip("[]"))
    return parts


def _quote_identifier(identifier: str) -> str:
    parts = _split_identifier_parts(identifier)
    if not parts:
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return ".".join(f"[{part}]" for part in parts)


_TYPE_DEFAULTS: dict[str, Any] = {
    "int": 0, "bigint": 0, "smallint": 0, "tinyint": 0,
    "bit": 0, "float": 0.0, "real": 0.0,
    "decimal": 0, "numeric": 0, "money": 0, "smallmoney": 0,
    "nvarchar": "", "varchar": "", "nchar": "", "char": "",
    "ntext": "", "text": "",
    "datetime": "1900-01-01", "datetime2": "1900-01-01",
    "smalldatetime": "1900-01-01", "date": "1900-01-01",
    "time": "00:00:00",
    "uniqueidentifier": "00000000-0000-0000-0000-000000000000",
    "varbinary": b"", "binary": b"", "image": b"",
    "xml": "",
}


def _get_not_null_defaults(cursor: Any, table: str) -> dict[str, Any]:
    """Return defaults for NOT NULL columns that lack a DEFAULT constraint.

    Queries INFORMATION_SCHEMA.COLUMNS to find NOT NULL columns without
    defaults, then maps each to a type-appropriate zero/empty value.
    Identity columns are excluded (they auto-generate).
    """
    try:
        cursor.execute(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "WHERE (c.TABLE_SCHEMA + '.' + c.TABLE_NAME = ? "
            "   OR '[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']' = ?) "
            "AND c.IS_NULLABLE = 'NO' "
            "AND c.COLUMN_DEFAULT IS NULL "
            "AND COLUMNPROPERTY(OBJECT_ID(?), c.COLUMN_NAME, 'IsIdentity') = 0",
            table, table, table,
        )
        defaults: dict[str, Any] = {}
        for col_name, data_type in cursor.fetchall():
            base_type = data_type.lower()
            if base_type in _TYPE_DEFAULTS:
                defaults[col_name] = _TYPE_DEFAULTS[base_type]
            else:
                defaults[col_name] = ""
        return defaults
    except _import_pyodbc().Error:
        logger.debug("event=not_null_defaults_lookup_failed table=%s", table)
        return {}


def _get_identity_columns(cursor: Any, table: str) -> set[str]:
    """Return the set of identity column names for *table* in the current DB.

    Queries ``sys.columns`` for ``is_identity = 1``.  Returns an empty set
    if the table has no identity columns or the query fails.
    """
    try:
        cursor.execute(
            "SELECT c.name FROM sys.columns c "
            "WHERE c.object_id = OBJECT_ID(?) AND c.is_identity = 1",
            table,
        )
        return {row[0] for row in cursor.fetchall()}
    except _import_pyodbc().Error:
        logger.debug(
            "event=identity_column_lookup_failed table=%s", table,
        )
        return set()


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    _validate_fixtures_base(fixtures, _validate_identifier)


_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|EXEC|EXECUTE|CREATE|ALTER|DROP|TRUNCATE)\b",
    re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    """Reject SQL that contains write operations.

    The refactored SQL must be a pure SELECT (WITH ... SELECT) statement.
    Raises ValueError if write keywords are detected.
    """
    _validate_readonly_sql_base(sql, _WRITE_SQL_RE)


def _detect_remote_exec_target(definition: str) -> dict[str, str] | None:
    for match in _REMOTE_EXEC_RE.finditer(definition):
        target = match.group("target")
        part_count = len(_split_identifier_parts(target))
        if part_count == 3:
            return {"kind": "cross-database", "target": target}
        if part_count == 4:
            return {"kind": "linked-server", "target": target}
    return None


class SqlServerSandbox(SandboxBackend):
    """Manage a throwaway SQL Server database for ground-truth capture."""

    def __init__(
        self,
        host: str,
        port: str,
        password: str,
        user: str = "sa",
        driver: str = "ODBC Driver 18 for SQL Server",
        *,
        source_host: str | None = None,
        source_port: str | None = None,
        source_database: str | None = None,
        source_user: str | None = None,
        source_password: str | None = None,
        source_driver: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.user = user
        self.driver = driver
        self.source_host = source_host or host
        self.source_port = source_port or port
        self.source_database = source_database or "master"
        self.source_user = source_user or user
        self.source_password = source_password or password
        self.source_driver = source_driver or driver

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> SqlServerSandbox:
        """Create an instance from strict runtime roles plus process secrets."""
        source_role = get_runtime_role(manifest, "source")
        sandbox_role = get_runtime_role(manifest, "sandbox")

        missing: list[str] = []
        if source_role is None:
            missing.append("runtime.source")
        if sandbox_role is None:
            missing.append("runtime.sandbox")
        if missing:
            raise ValueError(f"manifest.json is missing required runtime roles: {missing}")

        if source_role.technology != "sql_server":
            raise ValueError("runtime.source.technology must be sql_server for SQL Server sandbox")
        if sandbox_role.technology != "sql_server":
            raise ValueError("runtime.sandbox.technology must be sql_server for SQL Server sandbox")

        sandbox_host = sandbox_role.connection.host or ""
        sandbox_port = sandbox_role.connection.port or "1433"
        sandbox_user = sandbox_role.connection.user or ""
        sandbox_driver = sandbox_role.connection.driver or "FreeTDS"
        sandbox_password_env = sandbox_role.connection.password_env
        sandbox_password = os.environ.get(sandbox_password_env or "", "")

        source_host = source_role.connection.host or ""
        source_port = source_role.connection.port or "1433"
        source_database = source_role.connection.database or ""
        source_user = source_role.connection.user or "sa"
        source_driver = source_role.connection.driver or "FreeTDS"
        source_password_env = source_role.connection.password_env
        source_password = os.environ.get(source_password_env or "", "")

        if not sandbox_host:
            missing.append("runtime.sandbox.connection.host")
        if not sandbox_role.connection.port:
            missing.append("runtime.sandbox.connection.port")
        if not sandbox_user:
            missing.append("runtime.sandbox.connection.user")
        if not sandbox_password_env:
            missing.append("runtime.sandbox.connection.password_env")
        if not sandbox_password:
            missing.append(
                "environment variable referenced by runtime.sandbox.connection.password_env "
                f"({sandbox_password_env})"
            )
        if not source_host:
            missing.append("runtime.source.connection.host")
        if not source_role.connection.port:
            missing.append("runtime.source.connection.port")
        if not source_database:
            missing.append("runtime.source.connection.database")
        if not source_password_env:
            missing.append("runtime.source.connection.password_env")
        if not source_password:
            missing.append(
                "environment variable referenced by runtime.source.connection.password_env "
                f"({source_password_env})"
            )
        if missing:
            raise ValueError(f"Required sandbox configuration is missing: {missing}")

        return cls(
            host=sandbox_host,
            port=sandbox_port,
            password=sandbox_password,
            user=sandbox_user,
            driver=sandbox_driver,
            source_host=source_host,
            source_port=source_port,
            source_database=source_database,
            source_user=source_user,
            source_password=source_password,
            source_driver=source_driver,
        )

    @contextmanager
    def _connect(self, *, database: str | None = None) -> Generator[pyodbc.Connection, None, None]:
        db = database or "master"
        conn_str = build_sql_server_connection_string(
            host=self.host,
            port=self.port,
            database=db,
            user=self.user,
            password=self.password,
            driver=self.driver,
        )
        try:
            conn = _import_pyodbc().connect(conn_str, autocommit=True)
        except _import_pyodbc().Error as exc:
            msg = str(exc)
            if "Can't open lib" in msg:
                raise RuntimeError(
                    f"ODBC driver '{self.driver}' not found. "
                    "Install FreeTDS: brew install freetds"
                ) from exc
            raise
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _connect_source(
        self, *, database: str | None = None,
    ) -> Generator[pyodbc.Connection, None, None]:
        db = database or self.source_database
        conn = _import_pyodbc().connect(
            build_sql_server_connection_string(
                host=self.source_host,
                port=self.source_port,
                database=db,
                user=self.source_user,
                password=self.source_password,
                driver=self.source_driver,
            ),
            autocommit=True,
        )
        try:
            yield conn
        finally:
            conn.close()

    def _create_sandbox_db(
        self, sandbox_db: str,
    ) -> None:
        """Create the sandbox database, dropping it first if it exists."""
        _validate_sandbox_db_name(sandbox_db)
        quoted = f"[{sandbox_db}]"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DB_ID(?)", sandbox_db)
            exists = cursor.fetchone()[0] is not None
            if exists:
                cursor.execute(
                    f"ALTER DATABASE {quoted} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                )
                cursor.execute(f"DROP DATABASE {quoted}")
            cursor.execute(f"CREATE DATABASE {quoted}")
            logger.info("event=database_created sandbox_db=%s", sandbox_db)

    def _create_schemas(
        self, sandbox_cursor: pyodbc.Cursor, schemas: list[str],
    ) -> list[dict[str, str]]:
        """Create schemas in the sandbox. Returns list of error dicts."""
        errors: list[dict[str, str]] = []
        for schema in schemas:
            _validate_identifier(schema)
            try:
                sandbox_cursor.execute(
                    "SELECT 1 FROM sys.schemas WHERE name = ?", schema,
                )
                if sandbox_cursor.fetchone() is None:
                    sandbox_cursor.execute(f"CREATE SCHEMA [{schema}]")
            except _import_pyodbc().Error as exc:
                errors.append({
                    "code": "SCHEMA_CREATE_FAILED",
                    "message": f"Failed to create schema {schema}: {exc}",
                })
        return errors

    def _load_object_columns(
        self,
        source_cursor: pyodbc.Cursor,
        schema_name: str,
        object_name: str,
    ) -> list[dict[str, Any]]:
        identity_columns = _get_identity_columns(source_cursor, f"[{schema_name}].[{object_name}]")
        source_cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
            "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema_name,
            object_name,
        )
        return [
            {
                "name": row[0],
                "data_type": row[1],
                "char_len": row[2],
                "precision": row[3],
                "scale": row[4],
                "datetime_precision": row[5],
                "nullable": row[6] == "YES",
                "identity": row[0] in identity_columns,
            }
            for row in source_cursor.fetchall()
        ]

    @staticmethod
    def _render_column_type(column: dict[str, Any]) -> str:
        data_type = str(column["data_type"]).lower()
        char_len = column.get("char_len")
        precision = column.get("precision")
        scale = column.get("scale")
        datetime_precision = column.get("datetime_precision")

        if data_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            if char_len in (-1, None):
                return f"{data_type}(MAX)"
            return f"{data_type}({int(char_len)})"
        if data_type in {"decimal", "numeric"} and precision is not None:
            return f"{data_type}({int(precision)},{int(scale or 0)})"
        if data_type in {"datetime2", "datetimeoffset", "time"} and datetime_precision is not None:
            return f"{data_type}({int(datetime_precision)})"
        return data_type

    def _create_empty_table(
        self,
        sandbox_cursor: pyodbc.Cursor,
        schema_name: str,
        object_name: str,
        columns: list[dict[str, Any]],
    ) -> None:
        rendered = []
        for column in columns:
            line = f"[{column['name']}] {self._render_column_type(column)}"
            if column["identity"]:
                line += " IDENTITY(1,1)"
            line += " NULL" if column["nullable"] else " NOT NULL"
            rendered.append(line)
        sandbox_cursor.execute(
            f"CREATE TABLE [{schema_name}].[{object_name}] ({', '.join(rendered)})"
        )

    def _clone_tables(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structure from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ("
            + placeholders
            + ") ORDER BY TABLE_SCHEMA, TABLE_NAME",
            *schemas,
        )
        for schema_name, table_name in source_cursor.fetchall():
            _validate_identifier(schema_name)
            _validate_identifier(table_name)
            fqn = f"[{schema_name}].[{table_name}]"
            try:
                columns = self._load_object_columns(source_cursor, schema_name, table_name)
                self._create_empty_table(sandbox_cursor, schema_name, table_name, columns)
                cloned.append(f"{schema_name}.{table_name}")
            except _import_pyodbc().Error as exc:
                errors.append({
                    "code": "TABLE_CLONE_FAILED",
                    "message": f"Failed to clone {fqn}: {exc}",
                })
        return cloned, errors

    def _clone_views(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone view definitions from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.VIEWS "
            "WHERE TABLE_SCHEMA IN ("
            + placeholders
            + ") ORDER BY TABLE_SCHEMA, TABLE_NAME",
            *schemas,
        )
        for schema_name, view_name in source_cursor.fetchall():
            _validate_identifier(schema_name)
            _validate_identifier(view_name)
            fqn = f"[{schema_name}].[{view_name}]"
            source_cursor.execute(
                "SELECT OBJECT_DEFINITION(OBJECT_ID(?))",
                f"{schema_name}.{view_name}",
            )
            row = source_cursor.fetchone()
            definition = row[0] if row else None
            if definition is None:
                errors.append({
                    "code": "VIEW_DEFINITION_NULL",
                    "message": f"Cannot read definition for {fqn} (encrypted or inaccessible)",
                })
                continue
            try:
                sandbox_cursor.execute(definition)
                cloned.append(f"{schema_name}.{view_name}")
            except _import_pyodbc().Error as exc:
                errors.append({
                    "code": "VIEW_CLONE_FAILED",
                    "message": f"Failed to clone view {fqn}: {exc}",
                })
        return cloned, errors

    def _clone_procedures(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone procedure definitions from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT s.name AS schema_name, p.name AS proc_name, "
            "       OBJECT_DEFINITION(p.object_id) AS definition "
            "FROM sys.procedures p "
            "JOIN sys.schemas s ON p.schema_id = s.schema_id "
            "WHERE s.name IN ("
            + placeholders
            + ") ORDER BY s.name, p.name",
            *schemas,
        )
        for schema_name, proc_name, definition in source_cursor.fetchall():
            _validate_identifier(schema_name)
            _validate_identifier(proc_name)
            fqn = f"{schema_name}.{proc_name}"
            if definition is None:
                errors.append({
                    "code": "PROC_DEFINITION_NULL",
                    "message": f"Cannot read definition for {fqn} (encrypted or inaccessible)",
                })
                continue
            try:
                sandbox_cursor.execute(definition)
                cloned.append(fqn)
            except _import_pyodbc().Error as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone procedure {fqn}: {exc}",
                })
        return cloned, errors

    def _sandbox_clone_into(
        self,
        sandbox_db: str,
        schemas: list[str],
    ) -> SandboxUpOutput:
        _validate_identifier(self.source_database)
        _validate_sandbox_db_name(sandbox_db)

        errors: list[ErrorEntry] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            self._create_sandbox_db(sandbox_db)
            with self._connect(database=sandbox_db) as sandbox_conn, \
                 self._connect_source(database=self.source_database) as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()

                errors.extend(
                    ErrorEntry(**e)
                    for e in self._create_schemas(sandbox_cursor, schemas)
                )

                t_cloned, t_errors = self._clone_tables(
                    source_cursor, sandbox_cursor, schemas,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(ErrorEntry(**e) for e in t_errors)

                v_cloned, v_errors = self._clone_views(
                    source_cursor, sandbox_cursor, schemas,
                )
                views_cloned.extend(v_cloned)
                errors.extend(ErrorEntry(**e) for e in v_errors)

                p_cloned, p_errors = self._clone_procedures(
                    source_cursor, sandbox_cursor, schemas,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(ErrorEntry(**e) for e in p_errors)

        except _import_pyodbc().Error as exc:
            self.sandbox_down(sandbox_db)
            return SandboxUpOutput(
                sandbox_database=sandbox_db,
                status="error",
                tables_cloned=tables_cloned,
                views_cloned=views_cloned,
                procedures_cloned=procedures_cloned,
                errors=[ErrorEntry(code="SANDBOX_UP_FAILED", message=str(exc))],
            )

        status = "ok" if not errors else "partial"
        return SandboxUpOutput(
            sandbox_database=sandbox_db,
            status=status,
            tables_cloned=tables_cloned,
            views_cloned=views_cloned,
            procedures_cloned=procedures_cloned,
            errors=errors,
        )

    def sandbox_up(
        self,
        schemas: list[str],
    ) -> SandboxUpOutput:
        sandbox_db = generate_sandbox_name()
        logger.info(
            "event=sandbox_up sandbox_db=%s source=%s schemas=%s",
            sandbox_db, self.source_database, schemas,
        )
        result = self._sandbox_clone_into(sandbox_db, schemas)
        logger.info(
            "event=sandbox_up_complete sandbox_db=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db, result.status,
            len(result.tables_cloned), len(result.views_cloned),
            len(result.procedures_cloned), len(result.errors),
        )
        return result

    def sandbox_reset(
        self,
        sandbox_db: str,
        schemas: list[str],
    ) -> SandboxUpOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info(
            "event=sandbox_reset sandbox_db=%s source=%s schemas=%s",
            sandbox_db, self.source_database, schemas,
        )
        down_result = self.sandbox_down(sandbox_db)
        if down_result.status == "error":
            return SandboxUpOutput(
                sandbox_database=sandbox_db,
                status="error",
                tables_cloned=[],
                views_cloned=[],
                procedures_cloned=[],
                errors=[
                    ErrorEntry(
                        code="SANDBOX_RESET_FAILED",
                        message="Failed to drop existing sandbox before reset.",
                    ),
                    *down_result.errors,
                ],
            )

        result = self._sandbox_clone_into(sandbox_db, schemas)
        logger.info(
            "event=sandbox_reset_complete sandbox_db=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db, result.status,
            len(result.tables_cloned), len(result.views_cloned),
            len(result.procedures_cloned), len(result.errors),
        )
        return result

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info("event=sandbox_down sandbox_db=%s", sandbox_db)

        try:
            quoted = f"[{sandbox_db}]"
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_ID(?)", sandbox_db)
                exists = cursor.fetchone()[0] is not None
                if exists:
                    cursor.execute(
                        f"ALTER DATABASE {quoted} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                    )
                    cursor.execute(f"DROP DATABASE {quoted}")
            logger.info("event=sandbox_down_complete sandbox_db=%s", sandbox_db)
            return SandboxDownOutput(sandbox_database=sandbox_db, status="ok")
        except _import_pyodbc().Error as exc:
            logger.error("event=sandbox_down_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return SandboxDownOutput(
                sandbox_database=sandbox_db,
                status="error",
                errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message=str(exc))],
            )

    def sandbox_status(self, sandbox_db: str) -> SandboxStatusOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info("event=sandbox_status sandbox_db=%s", sandbox_db)

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_ID(?)", sandbox_db)
                exists = cursor.fetchone()[0] is not None

            if exists:
                logger.info("event=sandbox_status_complete sandbox_db=%s exists=true", sandbox_db)
                return SandboxStatusOutput(
                    sandbox_database=sandbox_db, status="ok", exists=True,
                )
            logger.info("event=sandbox_status_complete sandbox_db=%s exists=false", sandbox_db)
            return SandboxStatusOutput(
                sandbox_database=sandbox_db, status="not_found", exists=False,
            )
        except _import_pyodbc().Error as exc:
            logger.error("event=sandbox_status_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="error",
                exists=False,
                errors=[ErrorEntry(code="SANDBOX_STATUS_FAILED", message=str(exc))],
            )

    def _seed_fixtures(
        self,
        cursor: Any,
        sandbox_db: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables within an existing transaction.

        Handles trigger disabling, FK constraint toggling, and IDENTITY_INSERT.
        The caller is responsible for opening/rolling-back the transaction.

        **Important:** View-to-table replacement must be done *before* starting
        the transaction via ``_ensure_view_tables``, because DDL auto-commits
        in SQL Server and would break the rollback guarantee.
        """
        # Disable triggers on ALL user tables in the sandbox
        # before any INSERTs. Triggers may reference objects
        # that don't exist in the sandbox, causing spurious
        # failures during both fixture insertion and proc
        # execution.
        cursor.execute(
            "SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name) "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE t.is_ms_shipped = 0"
        )
        all_tables = [row[0] for row in cursor.fetchall()]
        for tbl in all_tables:
            cursor.execute(f"DISABLE TRIGGER ALL ON {tbl}")
        if all_tables:
            logger.info(
                "event=triggers_disabled sandbox_db=%s count=%d",
                sandbox_db, len(all_tables),
            )

        # Disable FK constraints on all fixture tables so
        # fixture rows can reference parent keys that may not
        # exist in the sandbox (e.g. NOT NULL FK defaults).
        fk_disabled_tables: list[str] = []
        for fixture in fixtures:
            table = fixture["table"]
            quoted_table = _quote_identifier(table)
            if fixture.get("rows"):
                cursor.execute(
                    f"ALTER TABLE {quoted_table} NOCHECK CONSTRAINT ALL"
                )
                fk_disabled_tables.append(table)
        if fk_disabled_tables:
            logger.info(
                "event=fk_constraints_disabled sandbox_db=%s tables=%s",
                sandbox_db, fk_disabled_tables,
            )

        for fixture in fixtures:
            table = fixture["table"]
            quoted_table = _quote_identifier(table)
            rows = fixture.get("rows", [])
            if not rows:
                continue
            fixture_columns = set(rows[0].keys())

            # Auto-fill NOT NULL columns missing from the fixture.
            # This lets fixtures specify only the columns the test
            # cares about — the sandbox fills in safe defaults for
            # the rest.
            not_null_defaults = _get_not_null_defaults(cursor, table)
            fill_cols = {
                col: default
                for col, default in not_null_defaults.items()
                if col not in fixture_columns
            }
            if fill_cols:
                logger.info(
                    "event=auto_fill_not_null sandbox_db=%s table=%s columns=%s",
                    sandbox_db, table, sorted(fill_cols.keys()),
                )

            columns = list(fixture_columns | fill_cols.keys())

            # Detect identity columns so we can toggle
            # IDENTITY_INSERT for explicit-value inserts.
            identity_cols = _get_identity_columns(cursor, table)
            needs_identity_insert = bool(
                identity_cols & fixture_columns
            )

            if needs_identity_insert:
                cursor.execute(
                    f"SET IDENTITY_INSERT {quoted_table} ON"
                )
                logger.info(
                    "event=identity_insert_enabled sandbox_db=%s "
                    "table=%s columns=%s",
                    sandbox_db,
                    table,
                    sorted(identity_cols & fixture_columns),
                )

            col_list = ", ".join(f"[{c}]" for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
            value_lists = [
                [row.get(c, fill_cols.get(c)) for c in columns]
                for row in rows
            ]
            cursor.executemany(insert_sql, value_lists)

            if needs_identity_insert:
                cursor.execute(
                    f"SET IDENTITY_INSERT {quoted_table} OFF"
                )

        # Re-enable FK constraints so subsequent operations
        # run under normal constraint rules.
        for table in fk_disabled_tables:
            cursor.execute(
                f"ALTER TABLE {table} CHECK CONSTRAINT ALL"
            )

    def _ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        """CTAS view-sourced fixtures as empty shell tables in the sandbox.

        For each entry in *given* whose object is a view in the source DB:
        drops the sandbox object (tolerating not-found), then CTASes it as an
        empty table so that fixture rows can be inserted normally.

        Uses autocommit connections so the DDL persists across the rollback
        that ends each scenario.  Idempotent within a sandbox lifetime because
        subsequent scenarios find the table already present.
        """
        materialized: list[str] = []
        with self._connect_source(database=self.source_database) as src_conn, self._connect(database=sandbox_db) as sb_conn:
            src_cur = src_conn.cursor()
            sb_cur = sb_conn.cursor()
            for fixture in given:
                parts = _split_identifier_parts(fixture["table"])
                if len(parts) != 2:
                    logger.warning(
                        "event=view_check_skipped reason=unexpected_parts fqn=%r parts=%d",
                        fixture["table"], len(parts),
                    )
                    continue
                schema_name, obj_name = parts
                src_cur.execute(
                    "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS "
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    schema_name, obj_name,
                )
                if src_cur.fetchone() is None:
                    continue  # base table — already cloned by _clone_tables
                fqn = f"[{schema_name}].[{obj_name}]"
                try:
                    sb_cur.execute(f"DROP TABLE IF EXISTS {fqn}")
                except _import_pyodbc().Error:
                    pass  # object did not exist; nothing to drop
                try:
                    sb_cur.execute(f"DROP VIEW IF EXISTS {fqn}")
                except _import_pyodbc().Error:
                    pass  # sandbox_up may not have cloned the view
                columns = self._load_object_columns(src_cur, schema_name, obj_name)
                self._create_empty_table(sb_cur, schema_name, obj_name, columns)
                materialized.append(f"{schema_name}.{obj_name}")
                logger.info(
                    "event=view_materialized sandbox_db=%s fqn=%s",
                    sandbox_db, fqn,
                )
        return materialized

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        _validate_sandbox_db_name(sandbox_db)

        scenario_name = scenario.get("name", "unnamed")
        for key in ("target_table", "procedure", "given"):
            if key not in scenario:
                raise KeyError(f"Scenario missing required key: {key!r}")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        _validate_identifier(target_table)
        _validate_identifier(procedure)
        _validate_fixtures(given)

        try:
            self._ensure_view_tables(sandbox_db, given)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s scenario=%s error=%s",
                sandbox_db, scenario_name, exc,
            )
            return TestHarnessExecuteOutput(
                scenario_name=scenario_name,
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="VIEW_MATERIALIZE_FAILED", message=str(exc))],
            )

        logger.info(
            "event=execute_scenario sandbox_db=%s scenario=%s procedure=%s",
            sandbox_db, scenario_name, procedure,
        )

        result_rows: list[dict[str, Any]] = []
        try:
            with self._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", procedure)
                    proc_definition_row = cursor.fetchone()
                    proc_definition = proc_definition_row[0] if proc_definition_row else None
                    remote_exec = (
                        _detect_remote_exec_target(proc_definition)
                        if proc_definition else None
                    )
                    if remote_exec:
                        target = remote_exec["target"]
                        kind = remote_exec["kind"]
                        logger.error(
                            "event=scenario_remote_exec_unsupported sandbox_db=%s scenario=%s "
                            "procedure=%s target=%s kind=%s",
                            sandbox_db, scenario_name, procedure, target, kind,
                        )
                        return TestHarnessExecuteOutput(
                            scenario_name=scenario_name,
                            status="error",
                            ground_truth_rows=[],
                            row_count=0,
                            errors=[ErrorEntry(
                                code="REMOTE_EXEC_UNSUPPORTED",
                                message=(
                                    f"Sandbox cannot execute {kind} procedure call "
                                    f"{target} from {procedure}. The sandbox only clones "
                                    "objects from the source database."
                                ),
                            )],
                        )

                    self._seed_fixtures(cursor, sandbox_db, given)

                    cursor.execute(f"EXEC {procedure}")

                    cursor.execute(f"SELECT * FROM {target_table}")
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=scenario_complete sandbox_db=%s scenario=%s rows=%d",
                sandbox_db, scenario_name, len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)

        except _import_pyodbc().Error as exc:
            logger.error(
                "event=scenario_failed sandbox_db=%s scenario=%s error=%s",
                sandbox_db, scenario_name, exc,
            )
            return build_execute_error(scenario_name, "SCENARIO_FAILED", str(exc))

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        _validate_sandbox_db_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql)

        scenario_name = "execute_select"
        logger.info("event=execute_select sandbox_db=%s", sandbox_db)

        try:
            self._ensure_view_tables(sandbox_db, fixtures)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s error=%s",
                sandbox_db, exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        result_rows: list[dict[str, Any]] = []
        try:
            with self._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql)
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=execute_select_complete sandbox_db=%s rows=%d",
                sandbox_db, len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=execute_select_failed sandbox_db=%s error=%s",
                sandbox_db, exc,
            )
            return build_execute_error(scenario_name, "EXECUTE_SELECT_FAILED", str(exc))

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        _validate_sandbox_db_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql_a)
        _validate_readonly_sql(sql_b)

        logger.info(
            "event=compare_two_sql sandbox_db=%s",
            sandbox_db,
        )

        try:
            self._ensure_view_tables(sandbox_db, fixtures)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s error=%s",
                sandbox_db, exc,
            )
            return build_compare_error("VIEW_MATERIALIZE_FAILED", str(exc))

        try:
            rows_a: list[dict[str, Any]] = []
            rows_b: list[dict[str, Any]] = []
            with self._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    # Syntax-check both SQL statements before seeding
                    # fixtures. PARSEONLY validates syntax without execution.
                    for label, sql in [("A", sql_a), ("B", sql_b)]:
                        try:
                            cursor.execute("SET PARSEONLY ON")
                            cursor.execute(sql)
                            cursor.execute("SET PARSEONLY OFF")
                        except _import_pyodbc().Error as parse_exc:
                            cursor.execute("SET PARSEONLY OFF")
                            conn.rollback()
                            logger.error(
                                "event=sql_syntax_error sandbox_db=%s label=%s error=%s",
                                sandbox_db, label, parse_exc,
                            )
                            return build_compare_error(
                                "SQL_SYNTAX_ERROR",
                                f"SQL {label} has syntax errors: {parse_exc}",
                            )

                    self._seed_fixtures(cursor, sandbox_db, fixtures)

                    cursor.execute(sql_a)
                    rows_a = _capture_rows_base(cursor)

                    cursor.execute(sql_b)
                    rows_b = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            result = build_compare_result(rows_a, rows_b)
            logger.info(
                "event=compare_two_sql_complete sandbox_db=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db,
                result["equivalent"],
                result["a_count"],
                result["b_count"],
            )
            return result

        except _import_pyodbc().Error as exc:
            logger.error(
                "event=compare_two_sql_failed sandbox_db=%s error=%s",
                sandbox_db, exc,
            )
            return build_compare_error("COMPARE_SQL_FAILED", str(exc))
