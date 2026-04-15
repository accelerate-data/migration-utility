import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.target_setup import SetupTargetOutput

runner = CliRunner()

_SETUP_TARGET_OUT = SetupTargetOutput(
    files=["dbt/dbt_project.yml"],
    sources_path="dbt/models/staging/sources.yml",
    target_source_schema="bronze",
    created_tables=["silver.DimCustomer"],
    existing_tables=[],
    desired_tables=["silver.DimCustomer"],
)


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1", "technology": "sql_server"}), encoding="utf-8"
    )


def test_setup_target_sql_server_writes_runtime_and_runs_orchestrator(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "sql_server", "bronze")


def test_setup_target_oracle_writes_runtime_and_runs_orchestrator(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "oracle", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "oracle", "bronze")


def test_setup_target_exits_1_on_missing_manifest(tmp_path):
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch(
            "shared.cli.setup_target_cmd.write_target_runtime_from_env",
            side_effect=ValueError("manifest.json not found"),
        ),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 1


def test_setup_target_rejects_snowflake(tmp_path):
    """Snowflake has no backend — must be rejected."""
    result = runner.invoke(
        app,
        ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1
