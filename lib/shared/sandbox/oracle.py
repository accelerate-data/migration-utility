"""Oracle sandbox backend using python-oracledb.

Oracle sandboxes are schema-level (not database-level): ``sandbox_up`` creates
a user ``__test_<hex>`` in the target PDB, clones tables and procedures from
the source schema via CTAS and ALL_SOURCE, then ``sandbox_down`` drops the
user with CASCADE.

Known limitations:
- ``RESOURCE`` role and ``UNLIMITED TABLESPACE`` are broad grants — acceptable
  for local sandbox use only.
- CTAS does not copy FK, PK, CHECK, or UNIQUE constraints. Fixture FK
  constraint disabling is therefore a no-op (and not needed).
- Partitioned tables are cloned as non-partitioned heap tables.
- Procedures referencing the source schema with fully-qualified names
  (e.g. ``SH.TABLENAME``) resolve to the source schema, not the sandbox.
  Write procedures using unqualified table names.
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
_ORA_SANDBOX_NAME_RE = re.compile(r"^__test_[a-zA-Z0-9_]{1,116}$")


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
    cursor: Any, sandbox_schema: str, table_name: str,
) -> dict[str, Any]:
    """Return safe defaults for NOT NULL columns absent from fixture rows.

    CTAS propagates NOT NULL constraints, so fixtures must supply values for
    all NOT NULL columns or this function fills them with type-appropriate
    zero/empty values.
    """
    try:
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2) AND NULLABLE = 'N'",
            [sandbox_schema, table_name],
        )
        defaults: dict[str, Any] = {}
        for col_name, data_type in cursor.fetchall():
            base_type = re.sub(r"\(.*\)", "", data_type.lower()).strip()
            defaults[col_name] = _ORA_TYPE_DEFAULTS.get(base_type, "")
        return defaults
    except _import_oracledb().DatabaseError:
        logger.debug(
            "event=oracle_not_null_defaults_failed schema=%s table=%s",
            sandbox_schema, table_name,
        )
        return {}


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    _validate_fixtures_base(fixtures, _validate_oracle_identifier)


_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|EXECUTE|CREATE|ALTER|DROP|TRUNCATE|CALL)\b",
    re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    """Reject SQL containing write operations.

    The refactored SQL must be a pure SELECT (WITH ... SELECT) statement.
    """
    _validate_readonly_sql_base(sql, _WRITE_SQL_RE)


class OracleSandbox(SandboxBackend):
    """Manage a throwaway Oracle schema for ground-truth capture.

    All operations use a single admin connection (SYS as SYSDBA by default),
    mirroring the SQL Server backend's use of ``sa`` for everything.

    ``execute_scenario`` and ``_seed_fixtures`` auto-qualify object names with
    the sandbox schema — callers pass bare object names (e.g. ``CHANNELS``),
    not schema-qualified names. ``compare_two_sql`` SQL must be schema-qualified
    by the caller when referencing sandbox objects.
    """

    def __init__(
        self,
        host: str,
        port: str,
        service: str,
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
        self.service = service
        self.password = password
        self.admin_user = admin_user
        self.source_schema = source_schema
        self.source_host = source_host or host
        self.source_port = source_port or port
        self.source_service = source_service or service
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
        service = sandbox_role.connection.service or ""
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
        if not service:
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
            service=service,
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
    def _connect(self) -> Generator[oracledb.Connection, None, None]:
        """Open an admin connection (SYSDBA when admin_user is ``sys``)."""
        dsn = f"{self.host}:{self.port}/{self.service}"
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
            # Set ISO date/timestamp formats so string literals like "1998-01-01"
            # bind correctly to DATE/TIMESTAMP columns in fixtures and queries.
            with conn.cursor() as cur:
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
                cur.execute(
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
            yield conn
        finally:
            conn.close()

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

        Sandbox names start with ``__`` so they must be double-quoted in Oracle
        DDL (unquoted identifiers must start with a letter). Quoted identifiers
        are stored verbatim (case-sensitive) in ALL_USERS, so all lookups use
        exact-case matching rather than UPPER().
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

    def _sandbox_clone_into(
        self,
        sandbox_schema: str,
        source_schema: str,
    ) -> SandboxUpOutput:
        _validate_oracle_identifier(source_schema)
        _validate_oracle_sandbox_name(sandbox_schema)

        errors: list[ErrorEntry] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            with self._connect() as sandbox_conn, self._connect_source() as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()
                self._create_sandbox_schema(sandbox_cursor, sandbox_schema)

                t_cloned, t_errors = self._clone_tables(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(ErrorEntry(**e) for e in t_errors)

                v_cloned, v_errors = self._clone_views(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                views_cloned.extend(v_cloned)
                errors.extend(ErrorEntry(**e) for e in v_errors)

                p_cloned, p_errors = self._clone_procedures(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(ErrorEntry(**e) for e in p_errors)

        except _import_oracledb().DatabaseError as exc:
            self.sandbox_down(sandbox_schema)
            return SandboxUpOutput(
                sandbox_database=sandbox_schema,
                status="error",
                tables_cloned=tables_cloned,
                views_cloned=views_cloned,
                procedures_cloned=procedures_cloned,
                errors=[ErrorEntry(code="SANDBOX_UP_FAILED", message=str(exc))],
            )

        status = "ok" if not errors else "partial"
        return SandboxUpOutput(
            sandbox_database=sandbox_schema,
            status=status,
            tables_cloned=tables_cloned,
            views_cloned=views_cloned,
            procedures_cloned=procedures_cloned,
            errors=errors,
        )

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        """Create sandbox schema and clone tables + procedures from source.

        The first element of ``schemas`` is used as the source schema name,
        overriding ``self.source_schema``. Subsequent elements are ignored —
        Oracle sandboxes are single-schema.
        """
        source_schema = schemas[0] if schemas else self.source_schema
        sandbox_schema = generate_sandbox_name()
        logger.info(
            "event=oracle_sandbox_up sandbox=%s source_schema=%s",
            sandbox_schema, source_schema,
        )
        result = self._sandbox_clone_into(sandbox_schema, source_schema)
        logger.info(
            "event=oracle_sandbox_up_complete sandbox=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_schema, result.status,
            len(result.tables_cloned), len(result.views_cloned),
            len(result.procedures_cloned), len(result.errors),
        )
        return result

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        logger.info(
            "event=oracle_sandbox_reset sandbox=%s schemas=%s",
            sandbox_db, schemas,
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

        source_schema = schemas[0] if schemas else self.source_schema
        result = self._sandbox_clone_into(sandbox_db, source_schema)
        logger.info(
            "event=oracle_sandbox_reset_complete sandbox=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db, result.status,
            len(result.tables_cloned), len(result.views_cloned),
            len(result.procedures_cloned), len(result.errors),
        )
        return result

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        logger.info("event=oracle_sandbox_down sandbox=%s", sandbox_db)

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                    [sandbox_db],
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute(f'DROP USER "{sandbox_db}" CASCADE')
            logger.info("event=oracle_sandbox_down_complete sandbox=%s", sandbox_db)
            return SandboxDownOutput(sandbox_database=sandbox_db, status="ok")
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_down_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return SandboxDownOutput(
                sandbox_database=sandbox_db,
                status="error",
                errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message=str(exc))],
            )

    def sandbox_status(self, sandbox_db: str) -> SandboxStatusOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        logger.info("event=oracle_sandbox_status sandbox=%s", sandbox_db)

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                    [sandbox_db],
                )
                exists = cursor.fetchone()[0] > 0

            if exists:
                logger.info(
                    "event=oracle_sandbox_status_complete sandbox=%s exists=true", sandbox_db,
                )
                return SandboxStatusOutput(
                    sandbox_database=sandbox_db, status="ok", exists=True,
                )
            logger.info(
                "event=oracle_sandbox_status_complete sandbox=%s exists=false", sandbox_db,
            )
            return SandboxStatusOutput(
                sandbox_database=sandbox_db, status="not_found", exists=False,
            )
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_status_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="error",
                exists=False,
                errors=[ErrorEntry(code="SANDBOX_STATUS_FAILED", message=str(exc))],
            )

    def _seed_fixtures(
        self,
        cursor: Any,
        sandbox_schema: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables.

        Table names in fixtures are bare identifiers (no schema prefix); this
        method qualifies them with ``sandbox_schema``. NOT NULL columns missing
        from fixture rows are auto-filled with safe defaults.

        CTAS does not copy FK constraints, so no FK disabling is required.

        **Important:** View-to-table replacement must be done *before* starting
        the transaction via ``_ensure_view_tables``, because DDL auto-commits
        in Oracle and would break the rollback guarantee.
        """
        for fixture in fixtures:
            table_name = fixture["table"]

            rows = fixture.get("rows", [])
            if not rows:
                continue

            fixture_columns = set(rows[0].keys())
            not_null_defaults = _get_oracle_not_null_defaults(
                cursor, sandbox_schema, table_name,
            )
            fill_cols = {
                col: default
                for col, default in not_null_defaults.items()
                if col not in fixture_columns
            }
            if fill_cols:
                logger.info(
                    "event=oracle_auto_fill_not_null sandbox=%s table=%s columns=%s",
                    sandbox_schema, table_name, sorted(fill_cols.keys()),
                )

            columns = list(fixture_columns | fill_cols.keys())
            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
            insert_sql = (
                f'INSERT INTO "{sandbox_schema}"."{table_name}" '
                f"({col_list}) VALUES ({placeholders})"
            )
            value_lists = [
                [row.get(c, fill_cols.get(c)) for c in columns]
                for row in rows
            ]
            cursor.executemany(insert_sql, value_lists)

    def _ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        """CTAS view-sourced fixtures as empty shell tables in the sandbox.

        For each entry in *given* whose object is a view in the source schema:
        drops the sandbox object (tolerating not-found), then CTASes it as an
        empty table so that fixture rows can be inserted normally.

        Oracle DDL auto-commits, so the shell table persists across the rollback
        that ends each scenario.  Idempotent within a sandbox lifetime because
        subsequent scenarios find the table already present.

        Table names in fixtures are bare identifiers (no schema prefix);
        this method qualifies them with ``self.source_schema`` for the source
        lookup and ``sandbox_db`` for the sandbox DDL.
        """
        materialized: list[str] = []
        with self._connect_source() as source_conn, self._connect() as sandbox_conn:
            source_cursor = source_conn.cursor()
            sandbox_cursor = sandbox_conn.cursor()
            for fixture in given:
                view_name = fixture["table"]
                _validate_oracle_identifier(view_name)
                source_cursor.execute(
                    "SELECT 1 FROM ALL_VIEWS "
                    "WHERE OWNER = UPPER(:1) AND VIEW_NAME = UPPER(:2)",
                    [self.source_schema, view_name],
                )
                if source_cursor.fetchone() is None:
                    continue  # base table — already cloned by _clone_tables
                try:
                    sandbox_cursor.execute(f'DROP TABLE "{sandbox_db}"."{view_name}"')
                except _import_oracledb().DatabaseError as exc:
                    logger.debug(
                        "event=oracle_view_drop_skipped sandbox=%s view=%s error=%s",
                        sandbox_db, view_name, exc,
                    )
                columns = self._load_object_columns(source_cursor, self.source_schema, view_name)
                self._create_empty_table(sandbox_cursor, sandbox_db, view_name, columns)
                materialized.append(view_name)
                logger.info(
                    "event=oracle_view_materialized sandbox=%s view=%s",
                    sandbox_db, view_name,
                )
        return materialized

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        """Run one test scenario: seed fixtures, execute procedure, capture output.

        ``procedure`` and ``target_table`` in the scenario are bare object names
        (no schema prefix) — this method qualifies them with ``sandbox_db``.
        Fixtures are rolled back after execution.
        """
        _validate_oracle_sandbox_name(sandbox_db)

        scenario_name = scenario.get("name", "unnamed")
        for key in ("target_table", "procedure", "given"):
            if key not in scenario:
                raise KeyError(f"Scenario missing required key: {key!r}")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        _validate_oracle_identifier(target_table)
        _validate_oracle_identifier(procedure)
        _validate_fixtures(given)

        try:
            self._ensure_view_tables(sandbox_db, given)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_view_materialize_failed sandbox=%s scenario=%s error=%s",
                sandbox_db, scenario_name, exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        logger.info(
            "event=oracle_execute_scenario sandbox=%s scenario=%s procedure=%s",
            sandbox_db, scenario_name, procedure,
        )

        result_rows: list[dict[str, Any]] = []
        try:
            with self._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._seed_fixtures(cursor, sandbox_db, given)
                    cursor.execute(
                        f'BEGIN "{sandbox_db}"."{procedure}"; END;'
                    )
                    cursor.execute(
                        f'SELECT * FROM "{sandbox_db}".{target_table}'
                    )
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=oracle_scenario_complete sandbox=%s scenario=%s rows=%d",
                sandbox_db, scenario_name, len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_scenario_failed sandbox=%s scenario=%s error=%s",
                sandbox_db, scenario_name, exc,
            )
            return build_execute_error(scenario_name, "SCENARIO_FAILED", str(exc))

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql)

        scenario_name = "execute_select"
        logger.info("event=oracle_execute_select sandbox=%s", sandbox_db)

        try:
            self._ensure_view_tables(sandbox_db, fixtures)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_view_materialize_failed sandbox=%s error=%s",
                sandbox_db, exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        result_rows: list[dict[str, Any]] = []
        try:
            with self._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql)
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=oracle_execute_select_complete sandbox=%s rows=%d",
                sandbox_db, len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_execute_select_failed sandbox=%s error=%s",
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
        """Seed fixtures, run two SELECT statements, return symmetric diff.

        Both SQL statements must be schema-qualified when referencing sandbox
        objects (e.g. ``SELECT * FROM "sandbox_schema"."TABLE"``).
        Fixtures and both queries are rolled back after comparison.
        """
        _validate_oracle_sandbox_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql_a)
        _validate_readonly_sql(sql_b)

        logger.info("event=oracle_compare_two_sql sandbox=%s", sandbox_db)

        for label, sql in (("A", sql_a), ("B", sql_b)):
            try:
                sqlglot.parse_one(sql, dialect="oracle")
            except sqlglot.errors.ParseError as exc:
                logger.error(
                    "event=oracle_sql_syntax_error sandbox=%s label=%s error=%s",
                    sandbox_db, label, exc,
                )
                return build_compare_error(
                    "SQL_SYNTAX_ERROR",
                    f"SQL {label} has syntax errors: {exc}",
                )

        try:
            self._ensure_view_tables(sandbox_db, fixtures)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=view_materialize_failed sandbox=%s error=%s",
                sandbox_db, exc,
            )
            return build_compare_error("VIEW_MATERIALIZE_FAILED", str(exc))

        try:
            rows_a: list[dict[str, Any]] = []
            rows_b: list[dict[str, Any]] = []
            with self._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql_a)
                    rows_a = _capture_rows_base(cursor)
                    cursor.execute(sql_b)
                    rows_b = _capture_rows_base(cursor)
                finally:
                    conn.rollback()
            result = build_compare_result(rows_a, rows_b)
            logger.info(
                "event=oracle_compare_two_sql_complete sandbox=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db, result["equivalent"], result["a_count"], result["b_count"],
            )
            return result
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_compare_two_sql_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return build_compare_error("COMPARE_SQL_FAILED", str(exc))
