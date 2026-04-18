import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.catalog_writer import WriteSeedOutput, WriteSourceOutput
from shared.loader_data import CatalogFileMissingError
from shared.output_models.dry_run import (
    DryRunOutput,
    ExcludeOutput,
    ObjectReadiness,
    ReadinessDetail,
    ResetMigrationOutput,
    ResetTargetResult,
)
from shared.output_models.sandbox import SandboxDownOutput

runner = CliRunner()


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"schema_version": "1"}), encoding="utf-8")


# ── reset ────────────────────────────────────────────────────────────────────

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


# ── reset all (global) ───────────────────────────────────────────────────────

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
            "catalog/tables/silver.dimcustomer.json:table.test_gen",
            "catalog/procedures/dbo.usp_load_dimcustomer.json:procedure.refactor",
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


# ── exclude-table ────────────────────────────────────────────────────────────

_EXCLUDE_OUT = ExcludeOutput(
    marked=["silver.AuditLog"],
    not_found=[],
    written_paths=["catalog/tables/silver.auditlog.json"],
)


def test_exclude_table_marks_tables(tmp_path):
    _write_manifest(tmp_path)

    with patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.auditlog.json" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output


# ── add-source-table ─────────────────────────────────────────────────────────


def test_add_source_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        object=ObjectReadiness(object="silver.audittest", ready=True, reason="scope complete"),
    )
    write_out = WriteSourceOutput(written="catalog/tables/silver.audittest.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out),
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.audittest.json" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output


def test_add_source_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        object=ObjectReadiness(object="silver.audittest", ready=False, reason="scope not complete"),
    )

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source") as mock_write,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_write.assert_not_called()
    assert "Review and commit the repo changes before continuing" not in result.output


def test_add_source_table_allows_seed_table_flip(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="not_applicable",
            code="SEED_TABLE",
            not_applicable=True,
        ),
    )
    write_out = WriteSourceOutput(written="catalog/tables/silver.lookup.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-source-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "source   silver.lookup" in result.output


# ── add-seed-table ────────────────────────────────────────────────────────────


def test_add_seed_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )
    write_out = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "seed     silver.lookup" in result.output
    assert "is_seed: true" in result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.lookup.json" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output


def test_add_seed_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="object_not_found",
            code="OBJECT_NOT_FOUND",
        ),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed") as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_not_called()
    assert "skipped  silver.lookup" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output


def test_add_seed_table_allows_source_table_flip(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="not_applicable",
            code="SOURCE_TABLE",
            not_applicable=True,
        ),
    )
    write_out = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "seed     silver.lookup" in result.output


def test_add_seed_table_warns_when_catalog_file_is_missing(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch(
            "shared.cli.add_seed_table_cmd.run_write_seed",
            side_effect=CatalogFileMissingError("table", "silver.lookup"),
        ),
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "missing  silver.lookup" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output


def test_add_seed_table_warns_when_write_fails_with_value_error(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", side_effect=ValueError("bad state")),
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "skipped  silver.lookup -- bad state" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output
