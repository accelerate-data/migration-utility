import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.dry_run import DryRunOutput, ExcludeOutput, ObjectReadiness, ReadinessDetail, ResetMigrationOutput
from shared.output_models.sandbox import SandboxDownOutput

runner = CliRunner()


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"schema_version": "1"}), encoding="utf-8")


# ── reset ────────────────────────────────────────────────────────────────────

_RESET_OUT = ResetMigrationOutput(
    stage="scope",
    targets=[],
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


def test_reset_all_with_sandbox_tears_down_before_reset(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="ok")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_backend.sandbox_down.assert_called_once_with("__test_abc")
    mock_reset.assert_called_once_with(tmp_path, "all", [])


def test_reset_all_sandbox_teardown_failure_aborts_before_reset(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1
    mock_reset.assert_not_called()


def test_reset_all_aborts_without_confirmation(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(app, ["reset", "all", "--project-root", str(tmp_path)], input="n\n")
    mock_reset.assert_not_called()
    assert result.exit_code == 0, result.output


def test_reset_all_rejects_fqn_arguments(tmp_path):
    _write_manifest(tmp_path)
    result = runner.invoke(app, ["reset", "all", "silver.Foo", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1


# ── exclude-table ────────────────────────────────────────────────────────────

_EXCLUDE_OUT = ExcludeOutput(marked=["silver.AuditLog"], not_found=[])


def test_exclude_table_marks_and_commits(tmp_path):
    _write_manifest(tmp_path)
    (tmp_path / "catalog").mkdir()
    (tmp_path / "catalog" / "tables").mkdir()

    with (
        patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT),
        patch("shared.cli.exclude_table_cmd.is_git_repo", return_value=True),
        patch("shared.cli.exclude_table_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_exclude_table_no_commit_flag(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT),
        patch("shared.cli.exclude_table_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--no-commit", "--project-root", str(tmp_path)],
        )
    mock_commit.assert_not_called()


# ── add-source-table ─────────────────────────────────────────────────────────

from shared.output_models.catalog_writer import WriteSourceOutput


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
        patch("shared.cli.add_source_table_cmd.is_git_repo", return_value=True),
        patch("shared.cli.add_source_table_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_add_source_table_no_commit_flag(tmp_path):
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
        patch("shared.cli.add_source_table_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--no-commit", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_commit.assert_not_called()


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
