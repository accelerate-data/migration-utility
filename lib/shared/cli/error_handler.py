"""cli_error_handler — clean error surface for ad-migration commands."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import typer

from shared.cli.output import console, error

logger = logging.getLogger(__name__)

# Guard driver imports — may not be installed in every environment.
try:
    import pyodbc as _pyodbc
    _PYODBC_INTERFACE_ERROR: type | None = _pyodbc.InterfaceError
    _PYODBC_PROGRAMMING_ERROR: type | None = _pyodbc.ProgrammingError
    _PYODBC_OPERATIONAL_ERROR: type | None = _pyodbc.OperationalError
    _PYODBC_ERROR: type | None = _pyodbc.Error
except ImportError:
    _PYODBC_INTERFACE_ERROR = None
    _PYODBC_PROGRAMMING_ERROR = None
    _PYODBC_OPERATIONAL_ERROR = None
    _PYODBC_ERROR = None

try:
    import oracledb as _oracledb
    _ORACLE_DATABASE_ERROR: type | None = _oracledb.DatabaseError
except ImportError:
    _ORACLE_DATABASE_ERROR = None


def _classify(exc: BaseException) -> tuple[int, str, str | None]:
    """Return (exit_code, message, hint_or_None) for a caught exception.

    pyodbc subtypes must be checked before the base pyodbc.Error class.
    """
    if _PYODBC_INTERFACE_ERROR and isinstance(exc, _PYODBC_INTERFACE_ERROR):
        return 2, str(exc), (
            "Install FreeTDS and unixODBC, then ensure FreeTDS is registered with unixODBC."
        )
    if _PYODBC_PROGRAMMING_ERROR and isinstance(exc, _PYODBC_PROGRAMMING_ERROR):
        return 2, str(exc), (
            "Verify SOURCE_MSSQL_DB is set to a valid database name and SOURCE_MSSQL_PASSWORD grants access"
        )
    if _PYODBC_OPERATIONAL_ERROR and isinstance(exc, _PYODBC_OPERATIONAL_ERROR):
        return 2, str(exc), "Check SOURCE_MSSQL_HOST, SOURCE_MSSQL_PORT, and network connectivity"
    if _PYODBC_ERROR and isinstance(exc, _PYODBC_ERROR):
        return 2, str(exc), "Check SQL Server connection environment variables"
    if _ORACLE_DATABASE_ERROR and isinstance(exc, _ORACLE_DATABASE_ERROR):
        return 2, str(exc), (
            "Check Oracle connection environment variables "
            "(SOURCE_ORACLE_HOST, SOURCE_ORACLE_PORT, SOURCE_ORACLE_USER, SOURCE_ORACLE_PASSWORD)"
        )
    if isinstance(exc, ConnectionError):
        return 2, str(exc), "Check host, port, and network access"
    if isinstance(exc, OSError):
        return 2, str(exc), "Check network connectivity or file path permissions"
    if isinstance(exc, ValueError):
        return 1, str(exc), None
    return 1, str(exc), None


@contextmanager
def cli_error_handler(operation: str) -> Iterator[None]:
    """Catch known exceptions and surface a clean error + hint instead of a traceback.

    Re-raises typer.Exit, typer.Abort, KeyboardInterrupt, and SystemExit unchanged
    so normal control flow is never interrupted.
    """
    try:
        yield
    except (typer.Exit, typer.Abort, KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        exit_code, message, hint = _classify(exc)
        prefix = "Unexpected error" if exit_code == 1 and not isinstance(exc, ValueError) else "Error"
        error(f"{prefix} while {operation}: {message}")
        if hint:
            console.print(f"  Hint: {hint}")
        logger.error(
            "event=cli_error component=error_handler operation=%s error_type=%s",
            operation,
            type(exc).__name__,
        )
        raise typer.Exit(code=exit_code)
