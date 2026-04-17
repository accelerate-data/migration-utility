"""Oracle sandbox backend using python-oracledb.

Oracle sandboxes are PDB-level (database-level): ``sandbox_up`` creates a
Pluggable Database ``SBX_<12 uppercase hex>`` via the CDB root connection,
creates schema users inside the PDB, then clones tables and procedures from
the source schema via explicit DDL and ALL_SOURCE.  ``sandbox_down`` closes
and drops the PDB including datafiles.

Known limitations:
- ``RESOURCE`` role and ``UNLIMITED TABLESPACE`` are broad grants — acceptable
  for local sandbox use only.
- Explicit DDL cloning does not copy FK, PK, CHECK, or UNIQUE constraints.
  Fixture FK constraint disabling is therefore a no-op (and not needed).
- Partitioned tables are cloned as non-partitioned heap tables.
- PDB names follow the ``SBX_<12 uppercase hex>`` pattern (16 chars), within
  Oracle's 30-character PDB name limit.
- No GENERATED AS IDENTITY handling; callers must omit identity columns.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import oracledb

import sqlglot

from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from pathlib import PurePosixPath

from shared.sandbox.base import (
    SandboxBackend,
    build_compare_error,
    build_compare_result,
    build_execute_error,
    build_execute_output,
    capture_rows as _capture_rows_base,
    validate_fixture_rows,
    validate_readonly_sql as _validate_readonly_sql_base,
)
from shared.runtime_config import get_runtime_role

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

logger = logging.getLogger(__name__)

# Oracle 23ai allows identifiers up to 128 bytes.
_ORA_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_$#][a-zA-Z0-9_$#]*$")
_ORA_SANDBOX_NAME_RE = re.compile(r"^SBX_[A-F0-9]{12}$")


def _generate_oracle_pdb_name() -> str:
    """Generate an Oracle-safe PDB name: ``SBX_<12 uppercase hex>``.

    Oracle rejects PDB names starting with ``__`` (ORA-65000), so PDBs use
    this generator instead of the generic ``generate_sandbox_name()``.
    """
    return f"SBX_{uuid.uuid4().hex[:12].upper()}"


def _validate_oracle_identifier(name: str) -> None:
    """Validate a bare Oracle identifier (no dots, no quotes).

    Rejects empty strings, SQL injection characters, and names exceeding
    Oracle's 128-character limit.
    """
    if not name:
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    if len(name) > 128:
        raise ValueError(f"Oracle identifier exceeds 128 chars: {name!r}")
    if re.search(r"[;'\"\\]", name):
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    if not _ORA_IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")


def _validate_oracle_qualified_name(name: str) -> None:
    """Validate a possibly schema-qualified Oracle name (``SCHEMA.TABLE``).

    Accepts bare identifiers (``CHANNELS``) and dotted qualified names
    (``MIGRATIONTEST.BRONZE_CURRENCY``).  Each segment must pass
    ``_validate_oracle_identifier``.
    """
    if not name:
        raise ValueError(f"Unsafe Oracle identifier: {name!r}")
    parts = name.split(".")
    for part in parts:
        _validate_oracle_identifier(part)


def _parse_qualified_name(name: str) -> tuple[str, str]:
    """Split ``SCHEMA.TABLE`` into ``(schema, table)``.

    If the name is unqualified (bare), raises ``ValueError`` — callers must
    always pass schema-qualified names.
    """
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
    cursor: Any, qualified_table: str,
) -> dict[str, Any]:
    """Return safe defaults for NOT NULL columns absent from fixture rows.

    ``qualified_table`` is a schema-qualified name like
    ``MIGRATIONTEST.BRONZE_CURRENCY``.  The schema and table parts are parsed
    and used to query ``ALL_TAB_COLUMNS``.

    CTAS propagates NOT NULL constraints, so fixtures must supply values for
    all NOT NULL columns or this function fills them with type-appropriate
    zero/empty values.
    """
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
    """Validate fixture structure: table names, column names, row consistency.

    Table names are schema-qualified (e.g. ``MIGRATIONTEST.BRONZE_CURRENCY``),
    so the table-name validator accepts dots.  Column names are bare
    identifiers validated with ``_validate_oracle_identifier``.
    """
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
    """Reject SQL containing write operations.

    The refactored SQL must be a pure SELECT (WITH ... SELECT) statement.
    """
    _validate_readonly_sql_base(sql, _WRITE_SQL_RE)


class _OracleSandboxCore(SandboxBackend):
    """Manage a throwaway Oracle schema for ground-truth capture.

    All operations use a single admin connection (SYS as SYSDBA by default),
    mirroring the SQL Server backend's use of ``sa`` for everything.

    Callers pass schema-qualified object names (e.g.
    ``MIGRATIONTEST.CHANNELS``) — the sandbox code uses them as-is.  The
    sandbox PDB is just a connection target.
    """

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

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> OracleSandbox:
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

        if source_role.technology != "oracle":
            raise ValueError("runtime.source.technology must be oracle for Oracle sandbox")
        if sandbox_role.technology != "oracle":
            raise ValueError("runtime.sandbox.technology must be oracle for Oracle sandbox")

        host = sandbox_role.connection.host or ""
        port = sandbox_role.connection.port or "1521"
        cdb_service = sandbox_role.connection.service or ""
        admin_user = sandbox_role.connection.user or ""
        password_env = sandbox_role.connection.password_env
        password = os.environ.get(password_env or "", "")
        source_host = source_role.connection.host or ""
        source_port = source_role.connection.port or "1521"
        source_service = source_role.connection.service or ""
        source_user = source_role.connection.user or ""
        source_password_env = source_role.connection.password_env
        source_password = os.environ.get(source_password_env or "", "")
        source_schema = source_role.connection.schema_name or ""

        if not host:
            missing.append("runtime.sandbox.connection.host")
        if not sandbox_role.connection.port:
            missing.append("runtime.sandbox.connection.port")
        if not cdb_service:
            missing.append("runtime.sandbox.connection.service")
        if not admin_user:
            missing.append("runtime.sandbox.connection.user")
        if not password_env:
            missing.append("runtime.sandbox.connection.password_env")
        if not password:
            missing.append(f"environment variable referenced by runtime.sandbox.connection.password_env ({password_env})")
        if not source_host:
            missing.append("runtime.source.connection.host")
        if not source_role.connection.port:
            missing.append("runtime.source.connection.port")
        if not source_service:
            missing.append("runtime.source.connection.service")
        if not source_user:
            missing.append("runtime.source.connection.user")
        if not source_password_env:
            missing.append("runtime.source.connection.password_env")
        if not source_password:
            missing.append(
                "environment variable referenced by runtime.source.connection.password_env "
                f"({source_password_env})"
            )
        if not source_schema:
            missing.append("runtime.source.connection.schema")
        if missing:
            raise ValueError(f"Required sandbox configuration is missing: {missing}")

        return cls(
            host=host,
            port=port,
            cdb_service=cdb_service,
            password=password,
            admin_user=admin_user,
            source_schema=source_schema,
            source_host=source_host,
            source_port=source_port,
            source_service=source_service,
            source_user=source_user,
            source_password=source_password,
        )

    @contextmanager
    def _connect_cdb(self) -> Generator[oracledb.Connection, None, None]:
        """Open an admin connection to the CDB root for PDB lifecycle DDL.

        No NLS session setup — this connection is only for CREATE/DROP
        PLUGGABLE DATABASE statements.
        """
        dsn = f"{self.host}:{self.port}/{self.cdb_service}"
        _ora = _import_oracledb()
        mode = (
            _ora.AUTH_MODE_SYSDBA
            if self.admin_user.lower() == "sys"
            else _ora.AUTH_MODE_DEFAULT
        )
        conn = _ora.connect(
            user=self.admin_user,
            password=self.password,
            dsn=dsn,
            mode=mode,
        )
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _connect_sandbox(self, sandbox_name: str) -> Generator[oracledb.Connection, None, None]:
        """Open an admin connection to a sandbox PDB by name.

        After ``_create_sandbox_pdb`` opens the PDB, Oracle auto-registers a
        service with the PDB name.  DSN = ``{host}:{port}/{sandbox_name}``.
        Sets NLS date/timestamp formats for fixture and query compatibility.
        """
        dsn = f"{self.host}:{self.port}/{sandbox_name}"
        _ora = _import_oracledb()
        mode = (
            _ora.AUTH_MODE_SYSDBA
            if self.admin_user.lower() == "sys"
            else _ora.AUTH_MODE_DEFAULT
        )
        conn = _ora.connect(
            user=self.admin_user,
            password=self.password,
            dsn=dsn,
            mode=mode,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
                cur.execute(
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
            yield conn
        finally:
            conn.close()

    def _create_sandbox_pdb(self, sandbox_name: str) -> None:
        """Create a pluggable database from pdbseed and open it.

        Uses ``_connect_cdb()`` to issue DDL against the CDB root.
        PDB names are ``SBX_<hex>`` — valid unquoted Oracle identifiers,
        so no double-quoting is needed.

        Oracle Free does not enable Oracle Managed Files (OMF) by default,
        so we discover the oradata directory from ``DBA_DATA_FILES`` and
        pass it as ``CREATE_FILE_DEST``.
        """
        _validate_oracle_sandbox_name(sandbox_name)
        temp_password = f"P{uuid.uuid4().hex[:16]}x"
        with self._connect_cdb() as conn:
            cursor = conn.cursor()

            # Discover oradata directory (go up two levels from first datafile)
            cursor.execute(
                "SELECT FILE_NAME FROM DBA_DATA_FILES WHERE ROWNUM = 1"
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("Cannot discover oradata path: DBA_DATA_FILES is empty")
            oradata_path = str(PurePosixPath(row[0]).parent.parent)

            cursor.execute(
                f"CREATE PLUGGABLE DATABASE {sandbox_name} "
                f'ADMIN USER pdb_admin IDENTIFIED BY "{temp_password}" '
                f"CREATE_FILE_DEST = '{oradata_path}'"
            )
            cursor.execute(
                f"ALTER PLUGGABLE DATABASE {sandbox_name} OPEN"
            )
        logger.info("event=oracle_sandbox_pdb_created sandbox=%s", sandbox_name)

    def _drop_sandbox_pdb(self, sandbox_name: str) -> None:
        """Close and drop a sandbox PDB including datafiles.

        Silently ignores errors if the PDB does not exist.
        PDB names are valid unquoted identifiers — no double-quoting needed.
        """
        _validate_oracle_sandbox_name(sandbox_name)
        try:
            with self._connect_cdb() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"ALTER PLUGGABLE DATABASE {sandbox_name} CLOSE IMMEDIATE"
                )
                cursor.execute(
                    f"DROP PLUGGABLE DATABASE {sandbox_name} INCLUDING DATAFILES"
                )
            logger.info("event=oracle_sandbox_pdb_dropped sandbox=%s", sandbox_name)
        except _import_oracledb().DatabaseError:
            logger.debug(
                "event=oracle_sandbox_pdb_drop_ignored sandbox=%s", sandbox_name,
            )

    @contextmanager
    def _connect_source(self) -> Generator[oracledb.Connection, None, None]:
        dsn = f"{self.source_host}:{self.source_port}/{self.source_service}"
        conn = _import_oracledb().connect(
            user=self.source_user,
            password=self.source_password,
            dsn=dsn,
        )
        try:
            yield conn
        finally:
            conn.close()

    def _create_sandbox_schema(self, cursor: Any, sandbox_schema: str) -> None:
        """Create sandbox user, dropping any prior instance first.

        ``SBX_<hex>`` names are valid unquoted Oracle identifiers but are
        double-quoted here for consistency.  ALL_USERS stores quoted names
        verbatim (case-sensitive), so lookups use exact-case matching.
        """
        temp_password = f"P{uuid.uuid4().hex[:16]}x"
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
            [sandbox_schema],
        )
        if cursor.fetchone()[0] > 0:
            cursor.execute(f'DROP USER "{sandbox_schema}" CASCADE')
            logger.info("event=oracle_sandbox_user_dropped sandbox=%s", sandbox_schema)
        cursor.execute(
            f'CREATE USER "{sandbox_schema}" IDENTIFIED BY "{temp_password}"'
        )
        cursor.execute(f'GRANT CONNECT, RESOURCE TO "{sandbox_schema}"')
        cursor.execute(f'GRANT UNLIMITED TABLESPACE TO "{sandbox_schema}"')
        logger.info("event=oracle_sandbox_user_created sandbox=%s", sandbox_schema)

    def _load_object_columns(
        self,
        source_cursor: Any,
        source_schema: str,
        object_name: str,
    ) -> list[dict[str, Any]]:
        source_cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE "
            "FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2) "
            "ORDER BY COLUMN_ID",
            [source_schema, object_name],
        )
        return [
            {
                "name": row[0],
                "data_type": row[1],
                "data_length": row[2],
                "data_precision": row[3],
                "data_scale": row[4],
                "nullable": row[5] == "Y",
            }
            for row in source_cursor.fetchall()
        ]

    @staticmethod
    def _render_column_type(column: dict[str, Any]) -> str:
        data_type = str(column["data_type"]).upper()
        data_length = column.get("data_length")
        data_precision = column.get("data_precision")
        data_scale = column.get("data_scale")

        if data_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"} and data_length:
            return f"{data_type}({int(data_length)})"
        if data_type == "NUMBER" and data_precision is not None:
            if data_scale is not None:
                return f"NUMBER({int(data_precision)},{int(data_scale)})"
            return f"NUMBER({int(data_precision)})"
        return data_type

    def _create_empty_table(
        self,
        sandbox_cursor: Any,
        sandbox_schema: str,
        object_name: str,
        columns: list[dict[str, Any]],
    ) -> None:
        rendered = [
            (
                f'"{column["name"]}" {self._render_column_type(column)} '
                f'{"NULL" if column["nullable"] else "NOT NULL"}'
            )
            for column in columns
        ]
        sandbox_cursor.execute(
            f'CREATE TABLE "{sandbox_schema}"."{object_name}" ({", ".join(rendered)})'
        )

    def _clone_tables(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structures from source to sandbox via explicit DDL."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT TABLE_NAME FROM ALL_TABLES "
            "WHERE OWNER = UPPER(:1) AND NESTED = 'NO' AND SECONDARY = 'N' "
            "ORDER BY TABLE_NAME",
            [source_schema],
        )
        table_names = [row[0] for row in source_cursor.fetchall()]

        for table_name in table_names:
            try:
                columns = self._load_object_columns(source_cursor, source_schema, table_name)
                self._create_empty_table(sandbox_cursor, sandbox_schema, table_name, columns)
                cloned.append(f"{source_schema}.{table_name}")
            except _import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "TABLE_CLONE_FAILED",
                    "message": f"Failed to clone {source_schema}.{table_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_table_clone_failed sandbox=%s table=%s error=%s",
                    sandbox_schema, table_name, exc,
                )

        return cloned, errors

    def _clone_views(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone view definitions from source to sandbox schema."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT VIEW_NAME, TEXT FROM ALL_VIEWS "
            "WHERE OWNER = UPPER(:1) ORDER BY VIEW_NAME",
            [source_schema],
        )
        views = source_cursor.fetchall()

        for view_name, view_text in views:
            _validate_oracle_identifier(view_name)
            ddl = (
                f'CREATE OR REPLACE VIEW "{sandbox_schema}"."{view_name}" AS '
                f"{view_text}"
            )
            try:
                sandbox_cursor.execute(ddl)
                cloned.append(f"{source_schema}.{view_name}")
            except _import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "VIEW_CLONE_FAILED",
                    "message": f"Failed to clone view {source_schema}.{view_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_view_clone_failed sandbox=%s view=%s error=%s",
                    sandbox_schema, view_name, exc,
                )

        return cloned, errors

    def _clone_procedures(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone procedure definitions from source to sandbox schema.

        Reads source text from ALL_SOURCE, prepends ``CREATE OR REPLACE``,
        and rewrites the owner prefix so the procedure is owned by the sandbox.
        Procedures must use unqualified table names to resolve correctly inside
        the sandbox.
        """
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT DISTINCT NAME FROM ALL_SOURCE "
            "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' ORDER BY NAME",
            [source_schema],
        )
        proc_names = [row[0] for row in source_cursor.fetchall()]

        for proc_name in proc_names:
            source_cursor.execute(
                "SELECT TEXT FROM ALL_SOURCE "
                "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' AND NAME = :2 "
                "ORDER BY LINE",
                [source_schema, proc_name],
            )
            lines = [row[0] for row in source_cursor.fetchall()]
            if not lines:
                errors.append({
                    "code": "PROC_DEFINITION_EMPTY",
                    "message": f"No source lines found for {source_schema}.{proc_name}",
                })
                continue

            full_source = "".join(lines)
            ddl = re.sub(
                rf"\bPROCEDURE\s+{re.escape(proc_name)}\b",
                f'PROCEDURE "{sandbox_schema}"."{proc_name}"',
                full_source,
                count=1,
                flags=re.IGNORECASE,
            )
            ddl = f"CREATE OR REPLACE {ddl.lstrip()}"

            try:
                sandbox_cursor.execute(ddl)
                cloned.append(f"{source_schema}.{proc_name}")
            except _import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone {source_schema}.{proc_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_proc_clone_failed sandbox=%s proc=%s error=%s",
                    sandbox_schema, proc_name, exc,
                )

        return cloned, errors
