from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.init import ScaffoldHooksOutput, ScaffoldProjectOutput

runner = CliRunner()

_SCAFFOLD_OUT = ScaffoldProjectOutput(files_created=["CLAUDE.md", ".envrc"], files_updated=[], files_skipped=[])
_HOOKS_OUT = ScaffoldHooksOutput(hook_created=True, hooks_path_configured=True)
_EXTRACT_OUT = {"tables": 5, "procedures": 3, "views": 2, "functions": 0}


def test_setup_source_sql_server_runs_extraction(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")

    with (
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
        patch("shared.cli.setup_source_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_source_cmd.stage_and_commit", return_value=True),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver,gold",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once_with(tmp_path, "AdventureWorks2022", ["silver", "gold"])


def test_setup_source_fails_fast_on_missing_env(tmp_path, monkeypatch):
    for var in ("MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB", "SA_PASSWORD"):
        monkeypatch.delenv(var, raising=False)

    result = runner.invoke(
        app,
        ["setup-source", "--technology", "sql_server", "--schemas", "silver",
         "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1


def test_setup_source_no_commit_flag(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "h")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT),
        patch("shared.cli.setup_source_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_source_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver",
             "--no-commit", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_commit.assert_not_called()
