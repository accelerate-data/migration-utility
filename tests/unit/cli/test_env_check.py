import pytest
from shared.cli.env_check import require_source_vars, require_sandbox_vars, require_target_vars


def test_require_source_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
    monkeypatch.setenv("SOURCE_MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "sa")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")
    require_source_vars("sql_server")  # must not raise or exit


def test_require_source_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PORT", "SOURCE_MSSQL_DB",
                "SOURCE_MSSQL_USER", "SOURCE_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("sql_server")
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "SOURCE_MSSQL_HOST" in err
    assert "SOURCE_MSSQL_PASSWORD" in err


def test_require_source_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "localhost")
    monkeypatch.setenv("SOURCE_ORACLE_PORT", "1521")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SOURCE_ORACLE_USER", "sh")
    monkeypatch.setenv("SOURCE_ORACLE_PASSWORD", "secret")
    require_source_vars("oracle")  # must not raise or exit


def test_require_source_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("SOURCE_ORACLE_HOST", "SOURCE_ORACLE_PORT", "SOURCE_ORACLE_SERVICE",
                "SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("oracle")
    assert exc_info.value.code == 1
    assert "SOURCE_ORACLE_HOST" in capsys.readouterr().err


def test_require_sandbox_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sa")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")
    require_sandbox_vars("sql_server")  # must not raise or exit


def test_require_sandbox_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("SANDBOX_MSSQL_HOST", "SANDBOX_MSSQL_PORT",
                "SANDBOX_MSSQL_USER", "SANDBOX_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("sql_server")
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "SANDBOX_MSSQL_HOST" in err
    assert "SANDBOX_MSSQL_PASSWORD" in err


def test_require_sandbox_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "localhost")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1521")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "admin")
    monkeypatch.setenv("SANDBOX_ORACLE_PASSWORD", "secret")
    require_sandbox_vars("oracle")  # must not raise or exit


def test_require_sandbox_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("SANDBOX_ORACLE_HOST", "SANDBOX_ORACLE_PORT", "SANDBOX_ORACLE_SERVICE",
                "SANDBOX_ORACLE_USER", "SANDBOX_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("oracle")
    assert exc_info.value.code == 1
    assert "SANDBOX_ORACLE_HOST" in capsys.readouterr().err


def test_require_target_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("TARGET_MSSQL_HOST", "localhost")
    monkeypatch.setenv("TARGET_MSSQL_PORT", "1433")
    monkeypatch.setenv("TARGET_MSSQL_DB", "target_db")
    monkeypatch.setenv("TARGET_MSSQL_USER", "sa")
    monkeypatch.setenv("TARGET_MSSQL_PASSWORD", "secret")
    require_target_vars("sql_server")  # must not raise or exit


def test_require_target_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_MSSQL_HOST", "TARGET_MSSQL_PORT", "TARGET_MSSQL_DB",
                "TARGET_MSSQL_USER", "TARGET_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("sql_server")
    assert exc_info.value.code == 1
    assert "TARGET_MSSQL_HOST" in capsys.readouterr().err


def test_require_target_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("TARGET_ORACLE_HOST", "localhost")
    monkeypatch.setenv("TARGET_ORACLE_PORT", "1521")
    monkeypatch.setenv("TARGET_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("TARGET_ORACLE_USER", "target_user")
    monkeypatch.setenv("TARGET_ORACLE_PASSWORD", "secret")
    require_target_vars("oracle")  # must not raise or exit


def test_require_target_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_ORACLE_HOST", "TARGET_ORACLE_PORT", "TARGET_ORACLE_SERVICE",
                "TARGET_ORACLE_USER", "TARGET_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("oracle")
    assert exc_info.value.code == 1
    assert "TARGET_ORACLE_HOST" in capsys.readouterr().err


def test_require_source_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("unknown_db")
    assert exc_info.value.code == 1
    assert "unknown_db" in capsys.readouterr().err


def test_require_sandbox_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("unknown_db")
    assert exc_info.value.code == 1
    assert "unknown_db" in capsys.readouterr().err


def test_require_target_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("unknown_platform")
    assert exc_info.value.code == 1
    assert "unknown_platform" in capsys.readouterr().err
