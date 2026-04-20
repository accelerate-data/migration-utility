from __future__ import annotations

from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app

runner = CliRunner()


def test_version_flag_prints_package_metadata_version() -> None:
    with patch("shared.cli.main._package_version", return_value="1.2.3"):
        result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "1.2.3"


def test_version_flag_falls_back_for_local_module_execution() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "0.1.2"
