"""SQL Server sandbox compatibility services and shared helpers."""

from __future__ import annotations

import logging
import re
from typing import Any

from shared.db_connect import SQL_SERVER_ODBC_DRIVER
from shared.sandbox.base import (
    SandboxBackend,
    validate_fixtures as _validate_fixtures_base,
    validate_readonly_sql as _validate_readonly_sql_base,
)

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
    """Validate a SQL identifier (schema, table, procedure name) is safe."""
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
    """Validate a sandbox database name is safe for interpolation."""
    if not re.match(r"^SBX_[A-F0-9]{12}$", sandbox_db):
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
    "int": 0,
    "bigint": 0,
    "smallint": 0,
    "tinyint": 0,
    "bit": 0,
    "float": 0.0,
    "real": 0.0,
    "decimal": 0,
    "numeric": 0,
    "money": 0,
    "smallmoney": 0,
    "nvarchar": "",
    "varchar": "",
    "nchar": "",
    "char": "",
    "ntext": "",
    "text": "",
    "datetime": "1900-01-01",
    "datetime2": "1900-01-01",
    "smalldatetime": "1900-01-01",
    "date": "1900-01-01",
    "time": "00:00:00",
    "uniqueidentifier": "00000000-0000-0000-0000-000000000000",
    "varbinary": b"",
    "binary": b"",
    "image": b"",
    "xml": "",
}


def _get_not_null_defaults(cursor: Any, table: str) -> dict[str, Any]:
    """Return defaults for NOT NULL columns that lack a DEFAULT constraint."""
    try:
        cursor.execute(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "WHERE (c.TABLE_SCHEMA + '.' + c.TABLE_NAME = ? "
            "   OR '[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']' = ?) "
            "AND c.IS_NULLABLE = 'NO' "
            "AND c.COLUMN_DEFAULT IS NULL "
            "AND COLUMNPROPERTY(OBJECT_ID(?), c.COLUMN_NAME, 'IsIdentity') = 0",
            table,
            table,
            table,
        )
        defaults: dict[str, Any] = {}
        for col_name, data_type in cursor.fetchall():
            base_type = data_type.lower()
            defaults[col_name] = _TYPE_DEFAULTS.get(base_type, "")
        return defaults
    except _import_pyodbc().Error:
        logger.debug("event=not_null_defaults_lookup_failed table=%s", table)
        return {}


def _get_identity_columns(cursor: Any, table: str) -> set[str]:
    """Return the set of identity column names for *table* in the current DB."""
    try:
        cursor.execute(
            "SELECT c.name FROM sys.columns c "
            "WHERE c.object_id = OBJECT_ID(?) AND c.is_identity = 1",
            table,
        )
        return {row[0] for row in cursor.fetchall()}
    except _import_pyodbc().Error:
        logger.debug("event=identity_column_lookup_failed table=%s", table)
        return set()


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    _validate_fixtures_base(fixtures, _validate_identifier)


_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|EXEC|EXECUTE|CREATE|ALTER|DROP|TRUNCATE)\b",
    re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    """Reject SQL that contains write operations."""
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


from shared.sandbox.sql_server_clone import SqlServerCloneMixin
from shared.sandbox.sql_server_config import SqlServerSandboxConfigMixin
from shared.sandbox.sql_server_connection import SqlServerConnectionMixin
from shared.sandbox.sql_server_lifecycle_core import SqlServerLifecycleCoreMixin


class _SqlServerSandboxCore(
    SqlServerSandboxConfigMixin,
    SqlServerConnectionMixin,
    SqlServerLifecycleCoreMixin,
    SqlServerCloneMixin,
    SandboxBackend,
):
    """Manage a throwaway SQL Server database for ground-truth capture."""

    def __init__(
        self,
        host: str,
        port: str,
        password: str,
        user: str = "sa",
        driver: str = SQL_SERVER_ODBC_DRIVER,
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


__all__ = [
    "_SqlServerSandboxCore",
    "_detect_remote_exec_target",
    "_get_identity_columns",
    "_get_not_null_defaults",
    "_import_pyodbc",
    "_quote_identifier",
    "_split_identifier_parts",
    "_validate_fixtures",
    "_validate_identifier",
    "_validate_readonly_sql",
    "_validate_sandbox_db_name",
]
