"""Oracle sandbox PDB and schema lifecycle helpers."""

from __future__ import annotations

import logging
import uuid
from pathlib import PurePosixPath
from typing import Any


def _services():
    from shared.sandbox import oracle_services

    return oracle_services

logger = logging.getLogger(__name__)


def _import_oracledb():
    return _services()._import_oracledb()


_ORA_DROP_IGNORABLE_CODES: frozenset[int] = frozenset((65011, 65020))


class OracleLifecycleCoreMixin:
    def _create_sandbox_pdb(self, sandbox_name: str) -> None:
        """Create a pluggable database from pdbseed and open it."""
        _services()._validate_oracle_sandbox_name(sandbox_name)
        temp_password = f"P{uuid.uuid4().hex[:16]}x"
        with self._connect_cdb() as conn:
            cursor = conn.cursor()
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
            try:
                cursor.execute(f"ALTER PLUGGABLE DATABASE {sandbox_name} OPEN")
            except _import_oracledb().DatabaseError:
                try:
                    self._drop_sandbox_pdb(sandbox_name)
                except _import_oracledb().DatabaseError:
                    logger.warning(
                        "event=oracle_sandbox_pdb_orphan sandbox=%s", sandbox_name
                    )
                raise
        logger.info("event=oracle_sandbox_pdb_created sandbox=%s", sandbox_name)

    def _drop_sandbox_pdb(self, sandbox_name: str) -> None:
        """Close and drop a sandbox PDB including datafiles."""
        _services()._validate_oracle_sandbox_name(sandbox_name)
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
        except _import_oracledb().DatabaseError as exc:
            ora_code = getattr(exc.args[0], "code", 0) if exc.args else 0
            if ora_code not in _ORA_DROP_IGNORABLE_CODES:
                raise
            logger.debug(
                "event=oracle_sandbox_pdb_drop_ignored sandbox=%s ora_code=%s",
                sandbox_name,
                ora_code,
            )

    def _create_sandbox_schema(self, cursor: Any, sandbox_schema: str) -> None:
        """Create sandbox user, dropping any prior instance first."""
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
