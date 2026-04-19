from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.dry_run import (
    ResetMigrationOutput,
    ResetTargetResult,
)
from shared.output_models.sandbox import SandboxDownOutput
from tests.unit.cli.helpers import _write_manifest

runner = CliRunner()


_RESET_OUT = ResetMigrationOutput(
    stage="scope",
    targets=[
        ResetTargetResult(
            fqn="silver.dimcustomer",
            status="reset",
            cleared_sections=["table.scoping"],
            mutated_files=["catalog/tables/silver.dimcustomer.json"],
        )
    ],
    reset=["silver.DimCustomer"],
    noop=[],
    blocked=[],
    not_found=[],
)

_GLOBAL_RESET_OUT = ResetMigrationOutput(
    stage="all",
    targets=[],
    reset=[],
    noop=[],
    blocked=[],
    not_found=[],
    deleted_paths=["catalog", "ddl", ".staging"],
    missing_paths=["test-specs", "dbt"],
    cleared_manifest_sections=["runtime.source", "runtime.target"],
)

def test_reset_runs_after_confirmation(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=_RESET_OUT) as mock_reset:
        result = runner.invoke(
            app,
            ["reset", "scope", "silver.DimCustomer", "--yes", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "scope", ["silver.DimCustomer"])
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.dimcustomer.json" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_reset_reports_procedure_catalog_mutations(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(
        stage="refactor",
        targets=[
            ResetTargetResult(
                fqn="silver.dimcustomer",
                status="reset",
                cleared_sections=["procedure:dbo.usp_load_dimcustomer.refactor"],
                mutated_files=["catalog/procedures/dbo.usp_load_dimcustomer.json"],
            )
        ],
        reset=["silver.dimcustomer"],
        noop=[],
        blocked=[],
        not_found=[],
    )
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(
            app,
            ["reset", "refactor", "silver.DimCustomer", "--yes", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "catalog/procedures/dbo.usp_load_dimcustomer.json" in result.output

def test_reset_noop_does_not_print_repo_state_or_reminder(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(
        stage="scope",
        targets=[ResetTargetResult(fqn="silver.dimcustomer", status="noop")],
        reset=[],
        noop=["silver.dimcustomer"],
        blocked=[],
        not_found=[],
    )
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(
            app,
            ["reset", "scope", "silver.DimCustomer", "--yes", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output

def test_reset_aborts_on_no(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(
            app,
            ["reset", "scope", "silver.DimCustomer", "--project-root", str(tmp_path)],
            input="n\n",
        )
    mock_reset.assert_not_called()
    assert result.exit_code == 0, result.output
    assert "Review and commit the repo changes before continuing" not in result.output

def test_reset_rejects_invalid_stage(tmp_path):
    result = runner.invoke(app, ["reset", "invalid-stage", "silver.Foo", "--yes",
                                  "--project-root", str(tmp_path)])
    assert result.exit_code == 1

def test_reset_exits_1_on_not_found(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=["silver.Missing"])
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(app, ["reset", "scope", "silver.Missing", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1
    assert "Review and commit the repo changes before continuing" not in result.output

def test_reset_exits_1_on_blocked(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(stage="scope", targets=[], reset=[], noop=[], blocked=["silver.Locked"], not_found=[])
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(app, ["reset", "scope", "silver.Locked", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1

def test_reset_all_no_sandbox_delegates_to_core(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value=None),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])
    assert "Updated repo state" in result.output
    assert "manifest.json" in result.output
    assert "catalog/" in result.output
    assert "ddl/" in result.output
    assert "Review and commit the repo changes before continuing" in result.output

def test_reset_all_preserve_catalog_delegates_to_core_and_reports_scope(tmp_path):
    _write_manifest(tmp_path)
    preserve_out = ResetMigrationOutput(
        stage="all",
        targets=[],
        reset=[],
        noop=[],
        blocked=[],
        not_found=[],
        deleted_paths=["dbt", "test-specs", ".staging", ".migration-runs"],
        missing_paths=[],
        cleared_manifest_sections=[],
        cleared_catalog_sections=[
            {
                "path": "catalog/tables/silver.dimcustomer.json",
                "section": "table.test_gen",
            },
            {
                "path": "catalog/procedures/dbo.usp_load_dimcustomer.json",
                "section": "procedure.refactor",
            },
        ],
        cleared_catalog_paths=[
            "catalog/tables/silver.dimcustomer.json",
            "catalog/procedures/dbo.usp_load_dimcustomer.json",
        ],
    )
    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value=None),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=preserve_out) as mock_reset,
    ):
        result = runner.invoke(
            app,
            ["reset", "all", "--preserve-catalog", "--yes", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [], preserve_catalog=True)
    assert "Preserve-catalog Reset Summary" in result.output
    assert "dbt" in result.output
    assert "catalog/tables/silver.dimcustomer.json:table.test_gen" in result.output
    assert "ddl/" not in result.output
    assert "Run setup-target" in result.output

def test_reset_all_preserve_catalog_does_not_teardown_sandbox_or_touch_manifest(tmp_path):
    _write_manifest(tmp_path)
    preserve_out = ResetMigrationOutput(
        stage="all",
        targets=[],
        reset=[],
        noop=[],
        blocked=[],
        not_found=[],
        deleted_paths=["dbt"],
    )
    with (
        patch("shared.cli.reset_cmd._load_manifest") as mock_load,
        patch("shared.cli.reset_cmd._create_backend") as mock_backend,
        patch("shared.cli.reset_cmd.clear_manifest_sandbox") as mock_clear,
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=preserve_out),
    ):
        result = runner.invoke(
            app,
            ["reset", "all", "--preserve-catalog", "--yes", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_load.assert_not_called()
    mock_backend.assert_not_called()
    mock_clear.assert_not_called()
    assert "manifest.json" not in result.output

def test_reset_all_preserve_catalog_catalog_error_exits_cleanly(tmp_path):
    _write_manifest(tmp_path)
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "catalog" / "tables" / "silver.dimcustomer.json").write_text(
        "{not valid json",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["reset", "all", "--preserve-catalog", "--yes", "--project-root", str(tmp_path)],
    )

    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)
    assert "Corrupt catalog JSON" in result.output

def test_reset_preserve_catalog_rejects_non_global_stage(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(
            app,
            [
                "reset",
                "scope",
                "silver.DimCustomer",
                "--preserve-catalog",
                "--yes",
                "--project-root",
                str(tmp_path),
            ],
        )

    assert result.exit_code == 1
    assert "--preserve-catalog is only supported with reset all" in result.output
    mock_reset.assert_not_called()

def test_reset_all_with_sandbox_tears_down_before_reset(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="SBX_ABC000000000", status="ok")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_backend.sandbox_down.assert_called_once_with("SBX_ABC000000000")
    mock_reset.assert_called_once_with(tmp_path, "all", [])

def test_reset_all_sandbox_teardown_failure_warns_and_continues(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="SBX_ABC000000000", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])

def test_reset_all_sandbox_teardown_failure_prints_manual_instructions(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="SBX_ABC000000000", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT),
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert "SBX_ABC000000000" in result.output

def test_reset_all_sandbox_teardown_failure_clears_manifest_sandbox(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="SBX_ABC000000000", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox") as mock_clear,
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT),
    ):
        runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    mock_clear.assert_called_once()

def test_reset_all_aborts_without_confirmation(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(app, ["reset", "all", "--project-root", str(tmp_path)], input="n\n")
    mock_reset.assert_not_called()
    assert result.exit_code == 0, result.output
    assert "Review and commit the repo changes before continuing" not in result.output

def test_reset_all_rejects_fqn_arguments(tmp_path):
    _write_manifest(tmp_path)
    result = runner.invoke(app, ["reset", "all", "silver.Foo", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1

def test_reset_all_sandbox_db_error_warns_and_continues(tmp_path):
    """A pyodbc error from sandbox_down is treated as a teardown failure — warn, continue."""
    import shared.cli.error_handler as _err_mod
    class _FakePyodbcProgramming(Exception): pass

    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        patch.multiple(
            _err_mod,
            _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
            _PYODBC_INTERFACE_ERROR=None,
            _PYODBC_OPERATIONAL_ERROR=None,
            _PYODBC_ERROR=_FakePyodbcProgramming,
        ),
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="SBX_ABC000000000"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Review and commit the repo changes before continuing" in result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])
