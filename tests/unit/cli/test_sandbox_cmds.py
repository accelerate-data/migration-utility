import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

import shared.cli.error_handler as _err_mod
from shared.cli.main import app
from shared.output_models.sandbox import SandboxDownOutput, SandboxUpOutput

runner = CliRunner()


def _write_manifest(tmp_path: Path, with_sandbox: bool = False) -> None:
    manifest = {
        "schema_version": "1",
        "technology": "sql_server",
        "runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {}}},
        "extraction": {"schemas": ["silver"]},
    }
    if with_sandbox:
        manifest["runtime"]["sandbox"] = {"technology": "sql_server", "dialect": "tsql", "connection": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


_SANDBOX_UP_OUT = SandboxUpOutput(
    sandbox_database="__test_abc123",
    status="ok",
    tables_cloned=["silver.DimCustomer"],
    views_cloned=[],
    procedures_cloned=["silver.usp_load"],
    errors=[],
)
_SANDBOX_DOWN_OUT = SandboxDownOutput(sandbox_database="__test_abc123", status="ok")


def test_setup_sandbox_runs_sandbox_up(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_up.assert_called_once()


def test_teardown_sandbox_requires_confirmation(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={"runtime": {"sandbox": {}}}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        # User enters 'n' at the prompt
        result = runner.invoke(app, ["teardown-sandbox", "--project-root", str(tmp_path)], input="n\n")

    assert result.exit_code == 0


def test_teardown_sandbox_yes_flag_skips_prompt(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0
    mock_backend.sandbox_down.assert_called_once_with("__test_abc123")


def test_teardown_sandbox_no_sandbox_exits_1(tmp_path):
    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value=None),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1


def test_teardown_sandbox_error_exits_nonzero(tmp_path):
    from shared.output_models.sandbox import SandboxDownOutput
    error_out = SandboxDownOutput(sandbox_database="__test_abc123", status="error")
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = error_out

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1


def test_setup_sandbox_commits_manifest(tmp_path):
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
        patch("shared.cli.setup_sandbox_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_sandbox_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_setup_sandbox_no_commit_skips_commit(tmp_path):
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
        patch("shared.cli.setup_sandbox_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_sandbox_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app, ["setup-sandbox", "--yes", "--no-commit", "--project-root", str(tmp_path)]
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_not_called()


def test_teardown_sandbox_commits_manifest(tmp_path):
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
        patch("shared.cli.teardown_sandbox_cmd.clear_manifest_sandbox"),
        patch("shared.cli.teardown_sandbox_cmd.is_git_repo", return_value=True),
        patch("shared.cli.teardown_sandbox_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_teardown_sandbox_no_commit_skips_commit(tmp_path):
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
        patch("shared.cli.teardown_sandbox_cmd.clear_manifest_sandbox"),
        patch("shared.cli.teardown_sandbox_cmd.is_git_repo", return_value=True),
        patch("shared.cli.teardown_sandbox_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app, ["teardown-sandbox", "--yes", "--no-commit", "--project-root", str(tmp_path)]
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_not_called()


def _patch_pyodbc_programming():
    class _FakePyodbcProgramming(Exception): pass
    return _FakePyodbcProgramming, patch.multiple(
        _err_mod,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_INTERFACE_ERROR=None,
        _PYODBC_OPERATIONAL_ERROR=None,
        _PYODBC_ERROR=_FakePyodbcProgramming,
    )


def test_setup_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_up.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output


def test_teardown_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output
