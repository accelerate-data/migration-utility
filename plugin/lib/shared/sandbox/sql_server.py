"""SQL Server sandbox backend using pyodbc."""

from __future__ import annotations

import logging
import os
import re
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pyodbc

from shared.db_connect import cursor_to_dicts
from shared.sandbox.base import SandboxBackend, serialize_rows, validate_fixture_rows

logger = logging.getLogger(__name__)

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


def _generate_sandbox_db_name() -> str:
    """Generate a random sandbox database name."""
    return f"__test_{uuid.uuid4().hex[:12]}"


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
            "WHERE c.TABLE_SCHEMA + '.' + c.TABLE_NAME = ? "
            "   OR '[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']' = ? "
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
    except Exception:  # noqa: BLE001
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
    except Exception:  # noqa: BLE001 — non-critical; caller falls back to plain INSERT
        logger.debug(
            "event=identity_column_lookup_failed table=%s", table,
        )
        return set()


def _validate_fixtures(fixtures: list[dict[str, Any]]) -> None:
    """Validate fixture structure: table names, column names, row consistency."""
    for fixture in fixtures:
        _validate_identifier(fixture["table"])
        rows = fixture.get("rows", [])
        if rows:
            for col_name in rows[0].keys():
                _validate_identifier(col_name)
            validate_fixture_rows(fixture["table"], rows)


_WRITE_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|EXEC|EXECUTE|CREATE|ALTER|DROP|TRUNCATE)\b",
    re.IGNORECASE,
)


def _validate_readonly_sql(sql: str) -> None:
    """Reject SQL that contains write operations.

    The refactored SQL must be a pure SELECT (WITH ... SELECT) statement.
    Raises ValueError if write keywords are detected.
    """
    if not sql or not sql.strip():
        raise ValueError("Refactored SQL is empty")
    if _WRITE_SQL_RE.search(sql):
        match = _WRITE_SQL_RE.search(sql)
        keyword = match.group(1) if match else "unknown"
        raise ValueError(
            f"Refactored SQL contains write operation '{keyword}'. "
            "Only SELECT/WITH statements are allowed."
        )


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
        database: str,
        password: str,
        user: str = "sa",
        driver: str = "ODBC Driver 18 for SQL Server",
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.password = password
        self.user = user
        self.driver = driver

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> SqlServerSandbox:
        """Create an instance from MSSQL_* env vars and manifest config.

        Raises ValueError if required configuration is missing.
        """
        host = os.environ.get("MSSQL_HOST", "")
        port = os.environ.get("MSSQL_PORT", "1433")
        database = manifest.get("source_database", os.environ.get("MSSQL_DB", ""))
        password = os.environ.get("SA_PASSWORD", "")
        user = os.environ.get("MSSQL_USER", "sa")
        driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

        missing = []
        if not host:
            missing.append("MSSQL_HOST")
        if not password:
            missing.append("SA_PASSWORD")
        if not database:
            missing.append("MSSQL_DB (or source_database in manifest)")
        if missing:
            raise ValueError(f"Required environment variables not set: {missing}")

        return cls(
            host=host, port=port, database=database, password=password,
            user=user, driver=driver,
        )

    @contextmanager
    def _connect(self, *, database: str | None = None) -> Generator[pyodbc.Connection, None, None]:
        db = database or self.database
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={db};"
            f"UID={self.user};PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=True)
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
            except pyodbc.Error as exc:
                errors.append({
                    "code": "SCHEMA_CREATE_FAILED",
                    "message": f"Failed to create schema {schema}: {exc}",
                })
        return errors

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
                sandbox_cursor.execute(
                    f"SELECT TOP 0 * INTO {fqn} "
                    f"FROM [{self.database}].{fqn}"
                )
                cloned.append(f"{schema_name}.{table_name}")
            except pyodbc.Error as exc:
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
            except pyodbc.Error as exc:
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
            except pyodbc.Error as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone procedure {fqn}: {exc}",
                })
        return cloned, errors

    def sandbox_up(
        self,
        schemas: list[str],
    ) -> dict[str, Any]:
        _validate_identifier(self.database)
        sandbox_db = _generate_sandbox_db_name()

        logger.info(
            "event=sandbox_up sandbox_db=%s source=%s schemas=%s",
            sandbox_db, self.database, schemas,
        )

        self._create_sandbox_db(sandbox_db)

        errors: list[dict[str, str]] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            with self._connect(database=sandbox_db) as sandbox_conn, \
                 self._connect(database=self.database) as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()

                errors.extend(self._create_schemas(sandbox_cursor, schemas))

                t_cloned, t_errors = self._clone_tables(
                    source_cursor, sandbox_cursor, schemas,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(t_errors)

                v_cloned, v_errors = self._clone_views(
                    source_cursor, sandbox_cursor, schemas,
                )
                views_cloned.extend(v_cloned)
                errors.extend(v_errors)

                p_cloned, p_errors = self._clone_procedures(
                    source_cursor, sandbox_cursor, schemas,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(p_errors)

        except pyodbc.Error as exc:
            logger.error("event=sandbox_up_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return {
                "sandbox_database": sandbox_db,
                "status": "error",
                "tables_cloned": tables_cloned,
                "views_cloned": views_cloned,
                "procedures_cloned": procedures_cloned,
                "errors": [{"code": "SANDBOX_UP_FAILED", "message": str(exc)}],
            }

        status = "ok" if not errors else "partial"
        logger.info(
            "event=sandbox_up_complete sandbox_db=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db, status, len(tables_cloned), len(views_cloned), len(procedures_cloned), len(errors),
        )
        return {
            "sandbox_database": sandbox_db,
            "status": status,
            "tables_cloned": tables_cloned,
            "views_cloned": views_cloned,
            "procedures_cloned": procedures_cloned,
            "errors": errors,
        }

    def sandbox_down(self, sandbox_db: str) -> dict[str, Any]:
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
            return {"sandbox_database": sandbox_db, "status": "ok"}
        except pyodbc.Error as exc:
            logger.error("event=sandbox_down_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return {
                "sandbox_database": sandbox_db,
                "status": "error",
                "errors": [{"code": "SANDBOX_DOWN_FAILED", "message": str(exc)}],
            }

    def sandbox_status(self, sandbox_db: str) -> dict[str, Any]:
        _validate_sandbox_db_name(sandbox_db)
        logger.info("event=sandbox_status sandbox_db=%s", sandbox_db)

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_ID(?)", sandbox_db)
                exists = cursor.fetchone()[0] is not None

            if exists:
                logger.info("event=sandbox_status_complete sandbox_db=%s exists=true", sandbox_db)
                return {
                    "sandbox_database": sandbox_db,
                    "status": "ok",
                    "exists": True,
                }
            logger.info("event=sandbox_status_complete sandbox_db=%s exists=false", sandbox_db)
            return {
                "sandbox_database": sandbox_db,
                "status": "not_found",
                "exists": False,
            }
        except pyodbc.Error as exc:
            logger.error("event=sandbox_status_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return {
                "sandbox_database": sandbox_db,
                "status": "error",
                "exists": False,
                "errors": [{"code": "SANDBOX_STATUS_FAILED", "message": str(exc)}],
            }

    def _seed_fixtures(
        self,
        cursor: Any,
        sandbox_db: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables within an existing transaction.

        Handles trigger disabling, FK constraint toggling, and IDENTITY_INSERT.
        The caller is responsible for opening/rolling-back the transaction.
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
            if fixture.get("rows"):
                cursor.execute(
                    f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL"
                )
                fk_disabled_tables.append(table)
        if fk_disabled_tables:
            logger.info(
                "event=fk_constraints_disabled sandbox_db=%s tables=%s",
                sandbox_db, fk_disabled_tables,
            )

        for fixture in fixtures:
            table = fixture["table"]

            # Detect views and replace with tables so fixture INSERTs work.
            cursor.execute(
                "SELECT OBJECTPROPERTY(OBJECT_ID(?), 'IsView')", table
            )
            is_view_row = cursor.fetchone()
            if is_view_row and is_view_row[0] == 1:
                cursor.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE, "
                    "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
                    "FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA + '.' + TABLE_NAME = PARSENAME(?, 2) + '.' + PARSENAME(?, 1) "
                    "ORDER BY ORDINAL_POSITION",
                    table, table,
                )
                col_defs = []
                for col_name, data_type, char_len, num_prec, num_scale in cursor.fetchall():
                    _validate_identifier(col_name)
                    if char_len is not None and char_len > 0:
                        col_defs.append(f"[{col_name}] {data_type}({char_len})")
                    elif num_prec is not None and num_scale is not None and data_type in ("decimal", "numeric"):
                        col_defs.append(f"[{col_name}] {data_type}({num_prec},{num_scale})")
                    else:
                        col_defs.append(f"[{col_name}] {data_type}")
                cursor.execute(f"DROP VIEW {table}")
                cursor.execute(
                    f"CREATE TABLE {table} ({', '.join(col_defs)})"
                )
                logger.info(
                    "event=view_replaced_with_table sandbox_db=%s table=%s columns=%d",
                    sandbox_db, table, len(col_defs),
                )

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
                    f"SET IDENTITY_INSERT {table} ON"
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
            insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
            value_lists = [
                [row.get(c, fill_cols.get(c)) for c in columns]
                for row in rows
            ]
            cursor.executemany(insert_sql, value_lists)

            if needs_identity_insert:
                cursor.execute(
                    f"SET IDENTITY_INSERT {table} OFF"
                )

        # Re-enable FK constraints so subsequent operations
        # run under normal constraint rules.
        for table in fk_disabled_tables:
            cursor.execute(
                f"ALTER TABLE {table} CHECK CONSTRAINT ALL"
            )

    @staticmethod
    def _capture_rows(cursor: Any) -> list[dict[str, Any]]:
        """Read all rows from the current cursor result set as dicts."""
        return cursor_to_dicts(cursor)

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
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
                        return {
                            "schema_version": "1.0",
                            "scenario_name": scenario_name,
                            "status": "error",
                            "ground_truth_rows": [],
                            "row_count": 0,
                            "errors": [{
                                "code": "REMOTE_EXEC_UNSUPPORTED",
                                "message": (
                                    f"Sandbox cannot execute {kind} procedure call "
                                    f"{target} from {procedure}. The sandbox only clones "
                                    "objects from the source database."
                                ),
                            }],
                        }

                    self._seed_fixtures(cursor, sandbox_db, given)

                    cursor.execute(f"EXEC {procedure}")

                    cursor.execute(f"SELECT * FROM {target_table}")
                    result_rows = self._capture_rows(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=scenario_complete sandbox_db=%s scenario=%s rows=%d",
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

        except pyodbc.Error as exc:
            logger.error(
                "event=scenario_failed sandbox_db=%s scenario=%s error=%s",
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
        _validate_sandbox_db_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql_a)
        _validate_readonly_sql(sql_b)

        logger.info(
            "event=compare_two_sql sandbox_db=%s",
            sandbox_db,
        )

        try:
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
                        except pyodbc.Error as parse_exc:
                            cursor.execute("SET PARSEONLY OFF")
                            logger.error(
                                "event=sql_syntax_error sandbox_db=%s label=%s error=%s",
                                sandbox_db, label, parse_exc,
                            )
                            return {
                                "status": "error",
                                "equivalent": False,
                                "a_count": 0,
                                "b_count": 0,
                                "a_minus_b": [],
                                "b_minus_a": [],
                                "errors": [{
                                    "code": "SQL_SYNTAX_ERROR",
                                    "message": f"SQL {label} has syntax errors: {parse_exc}",
                                }],
                            }

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
                "event=compare_two_sql_complete sandbox_db=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db, diff["equivalent"],
                diff["a_count"], diff["b_count"],
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

        except pyodbc.Error as exc:
            logger.error(
                "event=compare_two_sql_failed sandbox_db=%s error=%s",
                sandbox_db, exc,
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


