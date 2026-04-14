import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.dry_run import ExcludeOutput, ResetMigrationOutput

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


def test_reset_rejects_invalid_stage(tmp_path):
    result = runner.invoke(app, ["reset", "invalid-stage", "silver.Foo", "--yes",
                                  "--project-root", str(tmp_path)])
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
    ready_out = {"ready": True, "reason": "scope complete"}
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


def test_add_source_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = {"ready": False, "reason": "scope not complete"}

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
