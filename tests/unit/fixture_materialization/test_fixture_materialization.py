"""Tests for shared MigrationTest fixture materialization orchestration."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import pytest

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole


def test_materialize_migration_test_uses_adapter_script_and_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    script = tmp_path / "tests/integration/sql_server/fixtures/materialize.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)

    captured: dict[str, object] = {}

    def fake_run(cmd, cwd, env, capture_output, text, check):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["check"] = check
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("SA_PASSWORD", "secret")

    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="MigrationTest", password_env="SA_PASSWORD"),
    )

    result = materialize_migration_test(role, tmp_path, extra_env={"EXTRA_FLAG": "1"})

    assert result.returncode == 0
    assert captured["cmd"] == [str(script)]
    assert captured["cwd"] == tmp_path
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["check"] is False
    assert captured["env"]["MSSQL_DB"] == "MigrationTest"
    assert captured["env"]["SA_PASSWORD"] == "secret"
    assert captured["env"]["EXTRA_FLAG"] == "1"


def test_materialize_migration_test_requires_existing_script(tmp_path: Path) -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="MigrationTest"),
    )

    with pytest.raises(FileNotFoundError, match="MigrationTest fixture script not found"):
        materialize_migration_test(role, tmp_path)


def test_materialize_migration_test_logs_sql_server_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script = tmp_path / "tests/integration/sql_server/fixtures/materialize.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)

    def fake_run(cmd, cwd, env, capture_output, text, check):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("SA_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            database="AdventureWorks2022",
            schema="MigrationTest",
            password_env="SA_PASSWORD",
        ),
    )

    with caplog.at_level(logging.INFO):
        materialize_migration_test(role, tmp_path)

    assert (
        "event=fixture_materialization_start technology=sql_server"
        in caplog.text
    )
    assert "database=AdventureWorks2022" in caplog.text
    assert "schema=MigrationTest" in caplog.text
    assert "event=fixture_materialization_finish technology=sql_server" in caplog.text
    assert "status=success" in caplog.text
    assert "returncode=0" in caplog.text


def test_materialize_migration_test_logs_oracle_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script = tmp_path / "tests/integration/oracle/fixtures/materialize.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\nexit 12\n", encoding="utf-8")
    script.chmod(0o755)

    def fake_run(cmd, cwd, env, capture_output, text, check):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(cmd, 12, stdout="", stderr="boom")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("ORACLE_PWD", "secret")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            service="FREEPDB1",
            schema="MIGRATIONTEST",
            password_env="ORACLE_PWD",
        ),
    )

    with caplog.at_level(logging.INFO):
        result = materialize_migration_test(role, tmp_path)

    assert result.returncode == 12
    assert "event=fixture_materialization_start technology=oracle" in caplog.text
    assert "service=FREEPDB1" in caplog.text
    assert "schema=MIGRATIONTEST" in caplog.text
    assert "event=fixture_materialization_finish technology=oracle" in caplog.text
    assert "status=failure" in caplog.text
    assert "returncode=12" in caplog.text


def test_sql_server_materializer_pyodbc_fallback_uses_shared_connection_builder() -> None:
    script_path = (
        Path(__file__).resolve().parents[3]
        / "tests/integration/sql_server/fixtures/materialize.sh"
    )
    script_text = script_path.read_text(encoding="utf-8")

    assert "from shared.db_connect import build_sql_server_connection_string" in script_text
    assert "build_sql_server_connection_string(" in script_text
    assert "PWD={os.environ['SA_PASSWORD']}" not in script_text
    assert 'sys.path.insert(0, str(Path(sys.argv[3]) / "lib"))' in script_text
    assert 'Path.cwd() / "lib"' not in script_text


def test_oracle_materializer_avoids_dropping_the_target_schema_user() -> None:
    script_path = (
        Path(__file__).resolve().parents[3]
        / "tests/integration/oracle/fixtures/materialize.sh"
    )
    script_text = script_path.read_text(encoding="utf-8")

    assert 'DROP USER "${ORACLE_SCHEMA}" CASCADE' not in script_text
    assert 'ALTER USER "${ORACLE_SCHEMA}" IDENTIFIED BY "${ORACLE_SCHEMA_PASSWORD}" ACCOUNT UNLOCK' in script_text
    assert "FROM all_objects" in script_text
    assert "owner = UPPER('${ORACLE_SCHEMA}')" in script_text


def test_oracle_materializer_exits_cleanly_when_no_cli_and_no_oracledb() -> None:
    script_path = (
        Path(__file__).resolve().parents[3]
        / "tests/integration/oracle/fixtures/materialize.sh"
    )
    script_text = script_path.read_text(encoding="utf-8")

    assert (
        "no Oracle CLI (SQLCL/sql or sqlplus) is installed and python package "
        "'oracledb' is unavailable for Oracle materialization"
    ) in script_text
    assert 'importlib.util.find_spec("oracledb")' not in script_text
    assert "\nrun_python_materialization\n" in script_text
