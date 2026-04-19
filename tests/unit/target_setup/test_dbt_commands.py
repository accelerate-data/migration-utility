"""Tests for target dbt command helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from shared.target_setup_support.dbt_commands import DbtCommandResult, run_dbt_validation_command


def test_dbt_commands_support_module_exports_command_helpers() -> None:
    assert DbtCommandResult
    assert callable(run_dbt_validation_command)


def test_run_dbt_validation_command_runs_selected_models(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    dbt_root = project_root / "dbt"
    dbt_root.mkdir(parents=True)
    completed = MagicMock(returncode=0, stdout="compiled", stderr="")

    with patch("shared.target_setup_support.dbt_commands.subprocess.run", return_value=completed) as mock_run:
        result = run_dbt_validation_command(project_root, "compile", ["stg_bronze__customer"])

    expected = [
        "dbt",
        "compile",
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
        "--select",
        "stg_bronze__customer",
    ]
    assert mock_run.call_args.args[0] == expected
    assert result == DbtCommandResult(ran=True, command=expected)


def test_run_dbt_validation_command_skips_without_selectors(tmp_path: Path) -> None:
    with patch("shared.target_setup_support.dbt_commands.subprocess.run") as mock_run:
        result = run_dbt_validation_command(tmp_path, "compile", [])

    mock_run.assert_not_called()
    assert result == DbtCommandResult(ran=False, command=[])
