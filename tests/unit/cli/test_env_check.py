import os
import pytest
from shared.cli.env_check import require_source_vars, require_target_vars


def test_require_source_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")
    require_source_vars("sql_server")  # should not raise or exit


def test_require_source_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB", "SA_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("sql_server")
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "MSSQL_HOST" in captured.err
    assert "MSSQL_PORT" in captured.err
    assert "SA_PASSWORD" in captured.err


def test_require_source_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("oracle")
    assert exc_info.value.code == 1
    assert "ORACLE_HOST" in capsys.readouterr().err


def test_require_target_vars_snowflake_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_ACCOUNT", "TARGET_DATABASE", "TARGET_SCHEMA",
                "TARGET_WAREHOUSE", "TARGET_USER", "TARGET_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("snowflake")
    assert exc_info.value.code == 1
