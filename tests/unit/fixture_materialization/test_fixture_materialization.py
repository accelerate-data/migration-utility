"""Tests for shared MigrationTest fixture materialization orchestration."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole


def test_materialize_migration_test_uses_adapter_script_and_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    script = tmp_path / "scripts/sql/duckdb/materialize-migration-test.sh"
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

    role = RuntimeRole(
        technology="duckdb",
        dialect="duckdb",
        connection=RuntimeConnection(path=".runtime/duckdb/source.duckdb"),
    )

    result = materialize_migration_test(role, tmp_path, extra_env={"EXTRA_FLAG": "1"})

    assert result.returncode == 0
    assert captured["cmd"] == [str(script)]
    assert captured["cwd"] == tmp_path
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["check"] is False
    assert captured["env"]["DUCKDB_PATH"] == str(tmp_path / ".runtime" / "duckdb" / "source.duckdb")
    assert captured["env"]["EXTRA_FLAG"] == "1"


def test_materialize_migration_test_requires_existing_script(tmp_path: Path) -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="MigrationTest"),
    )

    with pytest.raises(FileNotFoundError, match="MigrationTest fixture script not found"):
        materialize_migration_test(role, tmp_path)
