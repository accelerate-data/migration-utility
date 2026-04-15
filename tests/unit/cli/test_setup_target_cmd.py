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


def test_setup_target_writes_runtime_and_runs_orchestrator(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET_ACCOUNT", "acme.snowflakecomputing.com")
    monkeypatch.setenv("TARGET_DATABASE", "WAREHOUSE")
    monkeypatch.setenv("TARGET_SCHEMA", "bronze")
    monkeypatch.setenv("TARGET_WAREHOUSE", "COMPUTE_WH")
    monkeypatch.setenv("TARGET_USER", "loader")
    monkeypatch.setenv("TARGET_PASSWORD", "secret")
    _write_manifest(tmp_path)

    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT) as mock_setup,
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "snowflake", "bronze")
    mock_setup.assert_called_once_with(tmp_path)


def test_setup_target_exits_1_on_missing_manifest(tmp_path):
    # write_target_runtime_from_env raises ValueError when manifest.json is absent.
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch(
            "shared.cli.setup_target_cmd.write_target_runtime_from_env",
            side_effect=ValueError("manifest.json not found"),
        ),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 1
