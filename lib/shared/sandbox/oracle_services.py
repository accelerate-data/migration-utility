"""Oracle sandbox compatibility services and shared helpers."""

from __future__ import annotations

import logging
import re
from typing import Any

from shared.sandbox.base import (
    SandboxBackend,
    generate_sandbox_name,
    validate_fixture_rows,
    validate_readonly_sql as _validate_readonly_sql_base,
)

logger = logging.getLogger(__name__)


_oracledb = None


def _import_oracledb():
    """Lazy-import oracledb so the module can be imported without it installed."""
    global _oracledb
    if _oracledb is None:
        try:
            import oracledb
        except ImportError as exc:
            raise ImportError(
                "oracledb is required for Oracle connectivity. "
                "Install it with: uv pip install oracledb"
            ) from exc
        _oracledb = oracledb
    return _oracledb


_ORA_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_$#][a-zA-Z0-9_$#]*$")
_ORA_SANDBOX_NAME_RE = re.compile(r"^SBX_[A-F0-9]{12}$")


def _generate_oracle_pdb_name() -> str:
    """Generate an Oracle-safe PDB name via ``generate_sandbox_name()``."""
    return generate_sandbox_name()


def _validate_oracle_identifier(name: str) -> None:
    """Validate a bare Oracle identifier (no dots, no quotes)."""
    if not name:
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    if len(name) > 128:
        raise ValueError(f"Oracle identifier exceeds 128 chars: {name!r}")
    if re.search(r"[;'\"\\]", name):
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    if not _ORA_IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")


def _validate_oracle_qualified_name(name: str) -> None:
    """Validate a possibly schema-qualified Oracle name."""
    if not name:
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    parts = name.split(".")
    if len(parts) > 2:
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    for part in parts:
        _validate_oracle_identifier(part)


def _parse_qualified_name(name: str) -> tuple[str, str]:
    """Split ``SCHEMA.TABLE`` into ``(schema, table)``."""
    parts = name.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Expected schema-qualified name (SCHEMA.TABLE), got: {name!r}"
        )
    return parts[0], parts[1]


def _validate_oracle_sandbox_name(sandbox_schema: str) -> None:
    """Validate that a sandbox schema name is safe for DDL interpolation."""
    if not _ORA_SANDBOX_NAME_RE.match(sandbox_schema):
        raise ValueError(f"Invalid Oracle sandbox schema name: {sandbox_schema!r}")


_ORA_TYPE_DEFAULTS: dict[str, Any] = {
    "number": 0,
    "float": 0.0,
    "binary_float": 0.0,
    "binary_double": 0.0,
    "varchar2": "",
    "nvarchar2": "",
    "char": " ",
    "nchar": " ",
    "date": "1900-01-01",
    "timestamp": "1900-01-01 00:00:00",
    "raw": b"",
    "clob": "",
    "blob": b"",
}


def _get_oracle_not_null_defaults(
    cursor: Any,
    qualified_table: str,
) -> dict[str, Any]:
    """Return safe defaults for NOT NULL columns absent from fixture rows."""
    try:
        schema, table_name = _parse_qualified_name(qualified_table)
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2) AND NULLABLE = 'N'",
            [schema, table_name],
        )
        defaults: dict[str, Any] = {}
        for col_name, data_type in cursor.fetchall():
            base_type = re.sub(r"\(.*\)", "", data_type.lower()).strip()
            defaults[col_name] = _ORA_TYPE_DEFAULTS.get(base_type, "")
        return defaults
    except _import_oracledb().DatabaseError:
        logger.debug(
            "event=oracle_not_null_defaults_failed table=%s",
            qualified_table,
        )
        return {}


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    for fixture in fixtures:
        _validate_oracle_qualified_name(fixture["table"])
        rows = fixture.get("rows", [])
        if rows:
            for col_name in rows[0].keys():
                _validate_oracle_identifier(col_name)
            validate_fixture_rows(fixture["table"], rows)


_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|EXECUTE|CREATE|ALTER|DROP|TRUNCATE|CALL)\b",
    re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    """Reject SQL containing write operations."""
    _validate_readonly_sql_base(sql, _WRITE_SQL_RE)


from shared.sandbox.oracle_clone import OracleCloneMixin
from shared.sandbox.oracle_config import OracleSandboxConfigMixin
from shared.sandbox.oracle_connection import OracleConnectionMixin
from shared.sandbox.oracle_lifecycle_core import OracleLifecycleCoreMixin


class _OracleSandboxCore(
    OracleSandboxConfigMixin,
    OracleConnectionMixin,
    OracleLifecycleCoreMixin,
    OracleCloneMixin,
    SandboxBackend,
):
    """Manage a throwaway Oracle schema for ground-truth capture."""

    def __init__(
        self,
        host: str,
        port: str,
        cdb_service: str,
        password: str,
        admin_user: str = "sys",
        source_schema: str = "",
        *,
        source_host: str | None = None,
        source_port: str | None = None,
        source_service: str | None = None,
        source_user: str | None = None,
        source_password: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.cdb_service = cdb_service
        self.password = password
        self.admin_user = admin_user
        self.source_schema = source_schema
        self.source_host = source_host or host
        self.source_port = source_port or port
        self.source_service = source_service or cdb_service
        self.source_user = source_user or admin_user
        self.source_password = source_password or password


__all__ = [
    "_OracleSandboxCore",
    "_generate_oracle_pdb_name",
    "_get_oracle_not_null_defaults",
    "_import_oracledb",
    "_parse_qualified_name",
    "_validate_fixtures",
    "_validate_oracle_identifier",
    "_validate_oracle_qualified_name",
    "_validate_oracle_sandbox_name",
    "_validate_readonly_sql",
]
