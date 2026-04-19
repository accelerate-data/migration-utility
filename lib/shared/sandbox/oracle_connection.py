"""Oracle sandbox connection context managers."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import oracledb


def _import_oracledb():
    from shared.sandbox import oracle_services

    return oracle_services._import_oracledb()


class OracleConnectionMixin:
    @contextmanager
    def _connect_cdb(self) -> Generator[oracledb.Connection, None, None]:
        """Open an admin connection to the CDB root for PDB lifecycle DDL."""
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
        """Open an admin connection to a sandbox PDB by name."""
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
