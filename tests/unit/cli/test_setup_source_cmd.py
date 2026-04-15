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
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
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


def test_setup_source_all_schemas_requires_confirmation(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract") as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas",
             "--project-root", str(tmp_path)],
            input="n\n",
        )

    assert result.exit_code == 0
    mock_extract.assert_not_called()


def test_setup_source_all_schemas_yes_flag_skips_confirmation(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once()


def test_setup_source_all_schemas_discovers_and_extracts(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}, {"schema": "bronze"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out) as mock_list,
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_list.assert_called_once()
    mock_extract.assert_called_once_with(tmp_path, "AdventureWorks2022", ["silver", "gold", "bronze"])


def test_setup_source_all_schemas_prints_discovered_schemas(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert "silver" in result.output
    assert "gold" in result.output


def test_setup_source_all_schemas_empty_discovery_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value={"schemas": []}),
        patch("shared.cli.setup_source_cmd.run_extract") as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 1
    mock_extract.assert_not_called()


def test_setup_source_all_schemas_and_schemas_are_mutually_exclusive(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    result = runner.invoke(
        app,
        ["setup-source", "--technology", "sql_server", "--schemas", "silver", "--all-schemas",
         "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1


def test_setup_source_neither_schemas_nor_all_schemas_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with patch("shared.cli.setup_source_cmd._check_source_prereqs"):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 1


def test_setup_source_shows_clean_error_on_db_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "BadDB")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    import shared.cli.error_handler as _mod
    class _FakePyodbcProgramming(Exception): pass

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract",
              side_effect=_FakePyodbcProgramming("Cannot open database")),
        patch.object(_mod, "_PYODBC_PROGRAMMING_ERROR", _FakePyodbcProgramming),
        patch.object(_mod, "_PYODBC_INTERFACE_ERROR", None),
        patch.object(_mod, "_PYODBC_OPERATIONAL_ERROR", None),
        patch.object(_mod, "_PYODBC_ERROR", _FakePyodbcProgramming),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 2
    assert "Hint:" in result.output
    assert "MSSQL_DB" in result.output


def test_setup_source_oracle_passes_none_database(tmp_path, monkeypatch):
    monkeypatch.setenv("ORACLE_HOST", "localhost")
    monkeypatch.setenv("ORACLE_PORT", "1521")
    monkeypatch.setenv("ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("ORACLE_USER", "sh")
    monkeypatch.setenv("ORACLE_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "oracle", "--schemas", "sh",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once_with(tmp_path, None, ["sh"])
