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


def _write_manifest(tmp_path: Path, target_technology: str | None = "sql_server") -> None:
    runtime: dict[str, object] = {}
    if target_technology is not None:
        runtime["target"] = {
            "technology": target_technology,
            "dialect": "tsql" if target_technology == "sql_server" else "oracle",
        }
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "runtime": runtime}),
        encoding="utf-8",
    )


def test_setup_target_sql_server_uses_manifest_runtime_target(tmp_path):
    _write_manifest(tmp_path, "sql_server")
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "sql_server", "bronze")
    assert "Updated repo state" in result.output
    assert "manifest.json" in result.output
    assert "dbt/dbt_project.yml" in result.output
    assert "dbt/models/staging/sources.yml" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output


def test_setup_target_oracle_uses_manifest_runtime_target(tmp_path):
    _write_manifest(tmp_path, "oracle")
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "oracle", "bronze")


def test_setup_target_exits_1_on_missing_manifest(tmp_path):
    result = runner.invoke(
        app,
        ["setup-target", "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1
    assert "Run init-ad-migration first" in result.output
    assert "Review and commit the repo changes before continuing" not in result.output


def test_setup_target_exits_1_when_runtime_target_missing(tmp_path):
    _write_manifest(tmp_path, target_technology=None)

    result = runner.invoke(
        app,
        ["setup-target", "--project-root", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "runtime.target" in result.output
    assert "Run init-ad-migration first" in result.output
