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
from typing import Any

import oracledb

from shared.db_connect import cursor_to_dicts
from shared.sandbox.base import SandboxBackend, serialize_rows, validate_fixture_rows

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


def _generate_oracle_sandbox_name() -> str:
    """Generate a unique sandbox schema name in the ``__test_<hex>`` format."""
    return f"__test_{uuid.uuid4().hex[:12]}"


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
    except oracledb.DatabaseError:
        logger.debug(
            "event=oracle_not_null_defaults_failed schema=%s table=%s",
            sandbox_schema, table_name,
        )
        return {}


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    for fixture in fixtures:
        _validate_oracle_identifier(fixture["table"])
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
    if not sql or not sql.strip():
        raise ValueError("SQL is empty")
    if _WRITE_SQL_RE.search(sql):
        match = _WRITE_SQL_RE.search(sql)
        keyword = match.group(1) if match else "unknown"
        raise ValueError(
            f"SQL contains write operation '{keyword}'. "
            "Only SELECT/WITH statements are allowed."
        )


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
    ) -> None:
        self.host = host
        self.port = port
        self.service = service
        self.password = password
        self.admin_user = admin_user
        self.source_schema = source_schema

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> OracleSandbox:
        """Create an instance from ``ORACLE_*`` env vars and manifest config.

        Raises ``ValueError`` if required configuration is missing.
        """
        host = os.environ.get("ORACLE_HOST", "localhost")
        port = os.environ.get("ORACLE_PORT", "1521")
        service = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
        admin_user = os.environ.get("ORACLE_ADMIN_USER", "sys")
        password = os.environ.get("ORACLE_PWD", "")
        source_schema = manifest.get(
            "source_database", os.environ.get("ORACLE_SCHEMA", ""),
        )

        missing = []
        if not password:
            missing.append("ORACLE_PWD")
        if not source_schema:
            missing.append("ORACLE_SCHEMA (or source_database in manifest)")
        if missing:
            raise ValueError(f"Required environment variables not set: {missing}")

        return cls(
            host=host,
            port=port,
            service=service,
            password=password,
            admin_user=admin_user,
            source_schema=source_schema,
        )

    @contextmanager
    def _connect(self) -> Generator[oracledb.Connection, None, None]:
        """Open an admin connection (SYSDBA when admin_user is ``sys``)."""
        dsn = f"{self.host}:{self.port}/{self.service}"
        mode = (
            oracledb.AUTH_MODE_SYSDBA
            if self.admin_user.lower() == "sys"
            else oracledb.AUTH_MODE_DEFAULT
        )
        conn = oracledb.connect(
            user=self.admin_user,
            password=self.password,
            dsn=dsn,
            mode=mode,
        )
        # Set ISO date/timestamp formats so string literals like "1998-01-01"
        # bind correctly to DATE/TIMESTAMP columns in fixtures and queries.
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
            cur.execute(
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
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

    def _clone_tables(
        self,
        cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structures from source to sandbox via CTAS.

        Creates non-partitioned copies without FK, PK, or CHECK constraints.
        Partitioned source tables become regular heap tables.
        """
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        cursor.execute(
            "SELECT TABLE_NAME FROM ALL_TABLES "
            "WHERE OWNER = UPPER(:1) AND NESTED = 'NO' AND SECONDARY = 'N' "
            "ORDER BY TABLE_NAME",
            [source_schema],
        )
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            try:
                cursor.execute(
                    f'CREATE TABLE "{sandbox_schema}"."{table_name}" '
                    f'AS SELECT * FROM "{source_schema}"."{table_name}" WHERE 1=0'
                )
                cloned.append(f"{source_schema}.{table_name}")
            except oracledb.DatabaseError as exc:
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
        cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone view definitions from source to sandbox schema."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        cursor.execute(
            "SELECT VIEW_NAME, TEXT FROM ALL_VIEWS "
            "WHERE OWNER = UPPER(:1) ORDER BY VIEW_NAME",
            [source_schema],
        )
        views = cursor.fetchall()

        for view_name, view_text in views:
            _validate_oracle_identifier(view_name)
            ddl = (
                f'CREATE OR REPLACE VIEW "{sandbox_schema}"."{view_name}" AS '
                f"{view_text}"
            )
            try:
                cursor.execute(ddl)
                cloned.append(f"{source_schema}.{view_name}")
            except oracledb.DatabaseError as exc:
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
        cursor: Any,
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

        cursor.execute(
            "SELECT DISTINCT NAME FROM ALL_SOURCE "
            "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' ORDER BY NAME",
            [source_schema],
        )
        proc_names = [row[0] for row in cursor.fetchall()]

        for proc_name in proc_names:
            cursor.execute(
                "SELECT TEXT FROM ALL_SOURCE "
                "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' AND NAME = :2 "
                "ORDER BY LINE",
                [source_schema, proc_name],
            )
            lines = [row[0] for row in cursor.fetchall()]
            if not lines:
                errors.append({
                    "code": "PROC_DEFINITION_EMPTY",
                    "message": f"No source lines found for {source_schema}.{proc_name}",
                })
                continue

            full_source = "".join(lines)
            ddl = re.sub(
                rf"\bPROCEDURE\s+{re.escape(proc_name)}\b",
                f'PROCEDURE "{sandbox_schema}".{proc_name}',
                full_source,
                count=1,
                flags=re.IGNORECASE,
            )
            ddl = f"CREATE OR REPLACE {ddl.lstrip()}"

            try:
                cursor.execute(ddl)
                cloned.append(f"{source_schema}.{proc_name}")
            except oracledb.DatabaseError as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone {source_schema}.{proc_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_proc_clone_failed sandbox=%s proc=%s error=%s",
                    sandbox_schema, proc_name, exc,
                )

        return cloned, errors

    def sandbox_up(self, schemas: list[str]) -> dict[str, Any]:
        """Create sandbox schema and clone tables + procedures from source.

        The first element of ``schemas`` is used as the source schema name,
        overriding ``self.source_schema``. Subsequent elements are ignored —
        Oracle sandboxes are single-schema.
        """
        source_schema = schemas[0] if schemas else self.source_schema
        _validate_oracle_identifier(source_schema)
        sandbox_schema = _generate_oracle_sandbox_name()

        logger.info(
            "event=oracle_sandbox_up sandbox=%s source_schema=%s",
            sandbox_schema, source_schema,
        )

        errors: list[dict[str, str]] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                self._create_sandbox_schema(cursor, sandbox_schema)

                t_cloned, t_errors = self._clone_tables(cursor, sandbox_schema, source_schema)
                tables_cloned.extend(t_cloned)
                errors.extend(t_errors)

                v_cloned, v_errors = self._clone_views(cursor, sandbox_schema, source_schema)
                views_cloned.extend(v_cloned)
                errors.extend(v_errors)

                p_cloned, p_errors = self._clone_procedures(
                    cursor, sandbox_schema, source_schema,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(p_errors)

        except oracledb.DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_up_failed sandbox=%s error=%s", sandbox_schema, exc,
            )
            return {
                "sandbox_database": sandbox_schema,
                "status": "error",
                "tables_cloned": tables_cloned,
                "views_cloned": views_cloned,
                "procedures_cloned": procedures_cloned,
                "errors": [{"code": "SANDBOX_UP_FAILED", "message": str(exc)}],
            }

        status = "ok" if not errors else "partial"
        logger.info(
            "event=oracle_sandbox_up_complete sandbox=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_schema, status, len(tables_cloned), len(views_cloned),
            len(procedures_cloned), len(errors),
        )
        return {
            "sandbox_database": sandbox_schema,
            "status": status,
            "tables_cloned": tables_cloned,
            "views_cloned": views_cloned,
            "procedures_cloned": procedures_cloned,
            "errors": errors,
        }

    def sandbox_down(self, sandbox_db: str) -> dict[str, Any]:
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
            return {"sandbox_database": sandbox_db, "status": "ok"}
        except oracledb.DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_down_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return {
                "sandbox_database": sandbox_db,
                "status": "error",
                "errors": [{"code": "SANDBOX_DOWN_FAILED", "message": str(exc)}],
            }

    def sandbox_status(self, sandbox_db: str) -> dict[str, Any]:
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
                return {"sandbox_database": sandbox_db, "status": "ok", "exists": True}
            logger.info(
                "event=oracle_sandbox_status_complete sandbox=%s exists=false", sandbox_db,
            )
            return {"sandbox_database": sandbox_db, "status": "not_found", "exists": False}
        except oracledb.DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_status_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return {
                "sandbox_database": sandbox_db,
                "status": "error",
                "exists": False,
                "errors": [{"code": "SANDBOX_STATUS_FAILED", "message": str(exc)}],
            }

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
        """
        for fixture in fixtures:
            table_name = fixture["table"]

            # Detect views and replace with tables so fixture INSERTs work.
            cursor.execute(
                "SELECT COUNT(*) FROM ALL_VIEWS "
                "WHERE OWNER = UPPER(:1) AND VIEW_NAME = UPPER(:2)",
                [sandbox_schema, table_name],
            )
            is_view = cursor.fetchone()[0] > 0
            if is_view:
                cursor.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, "
                    "DATA_PRECISION, DATA_SCALE, CHAR_LENGTH "
                    "FROM ALL_TAB_COLUMNS "
                    "WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2) "
                    "ORDER BY COLUMN_ID",
                    [sandbox_schema, table_name],
                )
                col_defs = []
                for col_name, data_type, data_length, data_prec, data_scale, char_len in cursor.fetchall():
                    _validate_oracle_identifier(col_name)
                    if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"):
                        length = char_len if char_len and char_len > 0 else data_length
                        col_defs.append(f'"{col_name}" {data_type}({length})')
                    elif data_type == "NUMBER" and data_prec is not None:
                        if data_scale and data_scale > 0:
                            col_defs.append(f'"{col_name}" {data_type}({data_prec},{data_scale})')
                        else:
                            col_defs.append(f'"{col_name}" {data_type}({data_prec})')
                    else:
                        col_defs.append(f'"{col_name}" {data_type}')
                cursor.execute(
                    f'DROP VIEW "{sandbox_schema}"."{table_name}"'
                )
                cursor.execute(
                    f'CREATE TABLE "{sandbox_schema}"."{table_name}" '
                    f'({", ".join(col_defs)})'
                )
                logger.info(
                    "event=oracle_view_replaced_with_table sandbox=%s table=%s columns=%d",
                    sandbox_schema, table_name, len(col_defs),
                )

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

    @staticmethod
    def _capture_rows(cursor: Any) -> list[dict[str, Any]]:
        """Read all rows from the current cursor result set as dicts."""
        return cursor_to_dicts(cursor)

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
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
                        f'BEGIN "{sandbox_db}".{procedure}; END;'
                    )
                    cursor.execute(
                        f'SELECT * FROM "{sandbox_db}".{target_table}'
                    )
                    result_rows = self._capture_rows(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=oracle_scenario_complete sandbox=%s scenario=%s rows=%d",
                sandbox_db, scenario_name, len(result_rows),
            )
            return {
                "schema_version": "1.0",
                "scenario_name": scenario_name,
                "status": "ok",
                "ground_truth_rows": serialize_rows(result_rows),
                "row_count": len(result_rows),
                "errors": [],
            }
        except oracledb.DatabaseError as exc:
            logger.error(
                "event=oracle_scenario_failed sandbox=%s scenario=%s error=%s",
                sandbox_db, scenario_name, exc,
            )
            return {
                "schema_version": "1.0",
                "scenario_name": scenario_name,
                "status": "error",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [{"code": "SCENARIO_FAILED", "message": str(exc)}],
            }

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

        try:
            with self._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql_a)
                    rows_a = serialize_rows(self._capture_rows(cursor))
                    cursor.execute(sql_b)
                    rows_b = serialize_rows(self._capture_rows(cursor))
                finally:
                    conn.rollback()

            from shared.refactor import symmetric_diff

            diff = symmetric_diff(rows_a, rows_b)
            logger.info(
                "event=oracle_compare_two_sql_complete sandbox=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db, diff["equivalent"], diff["a_count"], diff["b_count"],
            )
            return {
                "status": "ok",
                "equivalent": diff["equivalent"],
                "a_count": diff["a_count"],
                "b_count": diff["b_count"],
                "a_minus_b": diff["a_minus_b"],
                "b_minus_a": diff["b_minus_a"],
                "errors": [],
            }
        except oracledb.DatabaseError as exc:
            logger.error(
                "event=oracle_compare_two_sql_failed sandbox=%s error=%s", sandbox_db, exc,
            )
            return {
                "status": "error",
                "equivalent": False,
                "a_count": 0,
                "b_count": 0,
                "a_minus_b": [],
                "b_minus_a": [],
                "errors": [{"code": "COMPARE_SQL_FAILED", "message": str(exc)}],
            }


