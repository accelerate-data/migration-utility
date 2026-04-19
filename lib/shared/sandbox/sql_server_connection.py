"""SQL Server sandbox connection context managers."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from shared.db_connect import build_sql_server_connection_string

if TYPE_CHECKING:
    import pyodbc


def _import_pyodbc():
    from shared.sandbox import sql_server_services

    return sql_server_services._import_pyodbc()


class SqlServerConnectionMixin:
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
                    "Install FreeTDS using your platform package manager and "
                    "ensure unixODBC can see it."
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
