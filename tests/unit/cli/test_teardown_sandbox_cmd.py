from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.sandbox import (
    SandboxDownOutput,
)
from tests.unit.cli.helpers import _patch_pyodbc_programming, _write_sandbox_manifest

runner = CliRunner()


_SANDBOX_DOWN_OUT = SandboxDownOutput(sandbox_database="SBX_ABC123000000", status="ok")

def test_teardown_sandbox_requires_confirmation(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={"runtime": {"sandbox": {}}}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="SBX_ABC123000000"),
    ):
        # User enters 'n' at the prompt
        result = runner.invoke(app, ["teardown-sandbox", "--project-root", str(tmp_path)], input="n\n")

    assert result.exit_code == 0
    assert "Review and commit the repo changes before continuing" not in result.output

def test_teardown_sandbox_yes_flag_skips_prompt(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="SBX_ABC123000000"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0
    mock_backend.sandbox_down.assert_called_once_with("SBX_ABC123000000")
    assert "Updated repo state" in result.output
    assert "manifest.json" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_teardown_sandbox_no_sandbox_exits_1(tmp_path):
    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value=None),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "Review and commit the repo changes before continuing" not in result.output

def test_teardown_sandbox_error_exits_nonzero(tmp_path):
    from shared.output_models.sandbox import SandboxDownOutput
    error_out = SandboxDownOutput(sandbox_database="SBX_ABC123000000", status="error")
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = error_out

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="SBX_ABC123000000"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1

def test_teardown_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output
