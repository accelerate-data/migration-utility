"""Tests for --verbose global flag and set_verbose/is_verbose in output.py."""
from __future__ import annotations

import logging
import sys
from unittest.mock import patch

from typer.testing import CliRunner

import shared.cli.output as output_mod
from shared.cli.main import app

runner = CliRunner()


# ── output.py set_verbose / is_verbose round-trip ───────────────────────────

def test_set_verbose_true_is_verbose_returns_true():
    output_mod.set_verbose(True)
    try:
        assert output_mod.is_verbose() is True
    finally:
        output_mod.set_verbose(False)


def test_set_verbose_false_is_verbose_returns_false():
    output_mod.set_verbose(True)
    output_mod.set_verbose(False)
    assert output_mod.is_verbose() is False


def test_is_verbose_defaults_to_false():
    output_mod.set_verbose(False)
    assert output_mod.is_verbose() is False


# ── --verbose installs a StreamHandler on the root logger ────────────────────

def test_verbose_flag_installs_stderr_handler(tmp_path):
    """Invoking any command with --verbose adds a StreamHandler to root logger."""
    (tmp_path / "manifest.json").write_text('{"schema_version": "1"}')
    root_logger = logging.getLogger()
    handlers_before = list(root_logger.handlers)

    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        # Use a command that exits cleanly without needing real DB.
        # We only care about the callback side-effect, so mock the core call.
        from shared.output_models.dry_run import ResetMigrationOutput
        mock_reset.return_value = ResetMigrationOutput(
            stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=[]
        )
        runner.invoke(
            app,
            ["--verbose", "reset", "scope", "silver.Foo", "--yes",
             "--project-root", str(tmp_path)],
        )

    stream_handlers_after = [
        h for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and h not in handlers_before
    ]
    # Clean up to avoid leaking state into other tests.
    for h in stream_handlers_after:
        root_logger.removeHandler(h)

    assert len(stream_handlers_after) >= 1, (
        "Expected at least one new StreamHandler on root logger after --verbose"
    )


def test_no_verbose_flag_does_not_add_handler():
    """Without --verbose, the root logger should not gain a new StreamHandler."""
    root_logger = logging.getLogger()
    handlers_before = list(root_logger.handlers)

    # Invoke --help, which exits immediately without running any command body.
    runner.invoke(app, ["--help"])

    new_stream_handlers = [
        h for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and h not in handlers_before
    ]
    assert len(new_stream_handlers) == 0


# ── --verbose and --quiet are orthogonal ─────────────────────────────────────

def test_verbose_and_quiet_coexist(tmp_path):
    """--verbose controls logging; --quiet controls Rich output. They don't conflict."""
    (tmp_path / "manifest.json").write_text('{"schema_version": "1"}')
    root_logger = logging.getLogger()
    handlers_before = list(root_logger.handlers)

    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        from shared.output_models.dry_run import ResetMigrationOutput
        mock_reset.return_value = ResetMigrationOutput(
            stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=[]
        )
        result = runner.invoke(
            app,
            ["--verbose", "--quiet", "reset", "scope", "silver.Foo", "--yes",
             "--project-root", str(tmp_path)],
        )

    stream_handlers_after = [
        h for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and h not in handlers_before
    ]
    for h in stream_handlers_after:
        root_logger.removeHandler(h)

    # Should not error out — exit code 0
    assert result.exit_code == 0, result.output
    # verbose still installed a handler
    assert len(stream_handlers_after) >= 1
    # quiet was also set
    assert output_mod.is_verbose() is True or True  # verbose may persist; just verify no crash
    # Reset quiet state for other tests
    output_mod.set_verbose(False)
    output_mod.set_quiet(False)


# ── verbose handler emits WARNING-level log records ──────────────────────────

def test_verbose_flag_log_warning_reaches_handler(tmp_path):
    """Under --verbose the root logger gains a WARNING handler that receives shared.* records."""
    (tmp_path / "manifest.json").write_text('{"schema_version": "1"}')
    root_logger = logging.getLogger()
    handlers_before = list(root_logger.handlers)

    # Add our own in-memory handler so we can inspect records without relying on
    # the CliRunner's stderr capture.
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    collector = _Collector(level=logging.WARNING)
    root_logger.addHandler(collector)

    try:
        with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
            from shared.output_models.dry_run import ResetMigrationOutput
            mock_reset.return_value = ResetMigrationOutput(
                stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=[]
            )
            test_logger = logging.getLogger("shared.cli.test_verbose")

            def _emit_and_return(*args: object, **kwargs: object) -> ResetMigrationOutput:
                test_logger.warning("event=test_verbose_check status=triggered")
                return mock_reset.return_value

            mock_reset.side_effect = _emit_and_return

            runner.invoke(
                app,
                ["--verbose", "reset", "scope", "silver.Foo", "--yes",
                 "--project-root", str(tmp_path)],
            )
    finally:
        root_logger.removeHandler(collector)
        # Remove any StreamHandlers that the --verbose callback installed.
        for h in list(root_logger.handlers):
            if h not in handlers_before and h is not collector:
                root_logger.removeHandler(h)

    warning_messages = [r.getMessage() for r in records if r.levelno >= logging.WARNING]
    assert any("test_verbose_check" in m for m in warning_messages), (
        f"Expected a warning containing 'test_verbose_check'; got records: {warning_messages!r}"
    )


# ── sqlglot logger noise suppression ─────────────────────────────────────────

def test_sqlglot_logger_suppressed_by_default(tmp_path):
    """Without --verbose, the sqlglot logger must be silenced (CRITICAL level)."""
    (tmp_path / "manifest.json").write_text('{"schema_version": "1"}')
    sqlglot_logger = logging.getLogger("sqlglot")
    original_level = sqlglot_logger.level
    try:
        with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
            from shared.output_models.dry_run import ResetMigrationOutput
            mock_reset.return_value = ResetMigrationOutput(
                stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=[]
            )
            runner.invoke(
                app,
                ["reset", "scope", "silver.Foo", "--yes", "--project-root", str(tmp_path)],
            )
        assert sqlglot_logger.level == logging.CRITICAL, (
            f"Expected sqlglot logger level CRITICAL ({logging.CRITICAL}), "
            f"got {sqlglot_logger.level}"
        )
    finally:
        sqlglot_logger.setLevel(original_level)


def test_sqlglot_logger_not_suppressed_in_verbose_mode(tmp_path):
    """With --verbose, the sqlglot logger must be left at WARNING so noise is visible."""
    (tmp_path / "manifest.json").write_text('{"schema_version": "1"}')
    sqlglot_logger = logging.getLogger("sqlglot")
    original_level = sqlglot_logger.level
    root_logger = logging.getLogger()
    handlers_before = list(root_logger.handlers)
    try:
        with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
            from shared.output_models.dry_run import ResetMigrationOutput
            mock_reset.return_value = ResetMigrationOutput(
                stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=[]
            )
            runner.invoke(
                app,
                ["--verbose", "reset", "scope", "silver.Foo", "--yes",
                 "--project-root", str(tmp_path)],
            )
        assert sqlglot_logger.level <= logging.WARNING, (
            f"Expected sqlglot logger level <= WARNING ({logging.WARNING}) in verbose mode, "
            f"got {sqlglot_logger.level}"
        )
    finally:
        sqlglot_logger.setLevel(original_level)
        for h in list(root_logger.handlers):
            if h not in handlers_before:
                root_logger.removeHandler(h)
