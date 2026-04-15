"""Tests for cli_error_handler — one per exception type."""
from __future__ import annotations

from unittest.mock import patch

import typer
from typer.testing import CliRunner

import shared.cli.error_handler as _mod
from shared.cli.error_handler import _classify, cli_error_handler

runner = CliRunner()


def _app_raising(exc: Exception) -> typer.Typer:
    """Build a one-command Typer app that raises exc inside cli_error_handler."""
    app = typer.Typer()

    @app.command()
    def cmd() -> None:
        with cli_error_handler("test operation"):
            raise exc

    return app


# ── _classify unit tests ─────────────────────────────────────────────────────

class _FakePyodbcBase(Exception): pass
class _FakePyodbcInterface(_FakePyodbcBase): pass
class _FakePyodbcProgramming(_FakePyodbcBase): pass
class _FakePyodbcOperational(_FakePyodbcBase): pass
class _FakeOracleDB(Exception): pass


def _patch_drivers():
    return patch.multiple(
        _mod,
        _PYODBC_INTERFACE_ERROR=_FakePyodbcInterface,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_OPERATIONAL_ERROR=_FakePyodbcOperational,
        _PYODBC_ERROR=_FakePyodbcBase,
        _ORACLE_DATABASE_ERROR=_FakeOracleDB,
    )


def test_classify_pyodbc_interface_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcInterface("driver missing"))
    assert code == 2
    assert "ODBC Driver" in hint


def test_classify_pyodbc_programming_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcProgramming("bad db"))
    assert code == 2
    assert "MSSQL_DB" in hint


def test_classify_pyodbc_operational_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcOperational("conn refused"))
    assert code == 2
    assert "MSSQL_HOST" in hint


def test_classify_pyodbc_base_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcBase("generic"))
    assert code == 2
    assert "SQL Server" in hint


def test_classify_oracle_database_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakeOracleDB("ora error"))
    assert code == 2
    assert "ORACLE_HOST" in hint


def test_classify_os_error():
    code, msg, hint = _classify(OSError("no such file"))
    assert code == 2
    assert "file path" in hint


def test_classify_connection_error():
    code, msg, hint = _classify(ConnectionError("refused"))
    assert code == 2
    assert "port" in hint


def test_classify_value_error():
    code, msg, hint = _classify(ValueError("bad input"))
    assert code == 1
    assert hint is None


def test_classify_unknown_exception():
    code, msg, hint = _classify(RuntimeError("oops"))
    assert code == 1
    assert hint is None


# ── cli_error_handler integration tests ─────────────────────────────────────

def test_handler_shows_operation_in_output():
    with _patch_drivers():
        app = _app_raising(_FakePyodbcProgramming("db error"))
        result = runner.invoke(app, [])
    assert "test operation" in result.output


def test_handler_shows_hint_for_connection_errors():
    with _patch_drivers():
        app = _app_raising(_FakePyodbcProgramming("db error"))
        result = runner.invoke(app, [])
    assert "Hint:" in result.output
    assert result.exit_code == 2


def test_handler_no_hint_for_value_error():
    app = _app_raising(ValueError("bad value"))
    result = runner.invoke(app, [])
    assert "Hint:" not in result.output
    assert result.exit_code == 1


def test_handler_exit_1_for_unknown_exception():
    app = _app_raising(RuntimeError("surprise"))
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


def test_handler_does_not_swallow_typer_exit():
    app = typer.Typer()

    @app.command()
    def cmd() -> None:
        with cli_error_handler("test"):
            raise typer.Exit(code=0)

    result = runner.invoke(app, [])
    assert result.exit_code == 0
