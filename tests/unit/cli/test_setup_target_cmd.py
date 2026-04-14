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
        patch("shared.cli.setup_target_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_target_cmd.stage_and_commit", return_value=True),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "snowflake", "bronze")
    mock_setup.assert_called_once_with(tmp_path)


def test_setup_target_no_commit(tmp_path, monkeypatch):
    _write_manifest(tmp_path)
    monkeypatch.setenv("TARGET_PATH", "/tmp/warehouse.duckdb")

    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env"),
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
        patch("shared.cli.setup_target_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_target_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "duckdb", "--no-commit", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_commit.assert_not_called()
