from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import (
    CatalogFileMissingError,
)

def test_run_write_scoping_rejects_invalid_candidate_shape() -> None:
    """write-scoping rejects malformed candidate entries with actionable schema errors."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        tables_dir = root / "catalog" / "tables"
        procs_dir = root / "catalog" / "procedures"
        tables_dir.mkdir(parents=True)
        procs_dir.mkdir(parents=True)
        (tables_dir / "dbo.t.json").write_text(
            json.dumps({
                "schema": "dbo",
                "name": "t",
                "columns": [],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        (procs_dir / "dbo.usp_load.json").write_text(
            json.dumps({
                "schema": "dbo",
                "name": "usp_load",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "mode": "deterministic",
                "routing_reasons": [],
            }),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="procedure_name"):
            discover.run_write_scoping(
                root,
                "dbo.T",
                {
                    "selected_writer": "dbo.usp_load",
                    "selected_writer_rationale": "Only writer candidate.",
                    "candidates": [{"procedure": "dbo.usp_load"}],
                },
            )

def test_run_write_statements_rejects_missing_required_source() -> None:
    """write-statements rejects statement payloads that do not satisfy schema."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        procs_dir = root / "catalog" / "procedures"
        procs_dir.mkdir(parents=True)
        (procs_dir / "dbo.usp_test.json").write_text(
            json.dumps({
                "schema": "dbo",
                "name": "usp_test",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "mode": "deterministic",
                "routing_reasons": [],
            }),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="source"):
            discover.run_write_statements(
                root,
                "dbo.usp_test",
                [{"action": "migrate", "sql": "SELECT 1"}],
            )

def test_run_write_view_scoping_happy_path() -> None:
    """write-view-scoping with analyzed status writes scoping to catalog/views/."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # Create minimal view catalog
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_test.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping = {
            "sql_elements": [{"type": "join", "detail": "INNER JOIN bronze.customer"}],
            "call_tree": {"reads_from": ["bronze.customer"], "views_referenced": []},
            "logic_summary": "Joins customer data.",
            "rationale": "Simple join view.",
            "warnings": [],
            "errors": [],
        }
        result = discover.run_write_view_scoping(root, "silver.vw_test", scoping)
        assert result["status"] == "ok"
        written = json.loads(Path(result["written"]).read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "analyzed"
        assert written["scoping"]["sql_elements"][0]["type"] == "join"

def test_run_write_view_scoping_rejects_status_key() -> None:
    """write-view-scoping rejects dicts that include a status key."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_test.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="status must not be passed"):
            discover.run_write_view_scoping(root, "silver.vw_test", {"status": "analyzed", "sql_elements": []})

def test_run_write_view_scoping_missing_catalog() -> None:
    """write-view-scoping raises CatalogFileMissingError when catalog file is absent."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        with pytest.raises(CatalogFileMissingError):
            discover.run_write_view_scoping(root, "silver.vw_missing", {"sql_elements": []})

def test_write_scoping_cli_auto_detects_view_catalog() -> None:
    """write-scoping CLI routes to view path when catalog/views/<fqn>.json exists.

    Uses the repo root (a valid git repo) as project-root. Creates and cleans up
    a temporary view catalog at catalog/views/silver.vw_cli_test.json.
    """
    import json as _json
    from typer.testing import CliRunner

    # Repo root is 4 levels up from this test file: tests/unit/test_discover.py
    repo_root = Path(__file__).resolve().parents[3]
    views_dir = repo_root / "catalog" / "views"
    cat_file = views_dir / "silver.vw_cli_test.json"
    scoping_file = repo_root / ".staging-test-scoping.json"
    try:
        views_dir.mkdir(parents=True, exist_ok=True)
        cat_file.write_text(
            _json.dumps({
                "schema": "silver", "name": "vw_cli_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping_file.write_text(
            _json.dumps({"sql_elements": [], "warnings": [], "errors": []}),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(
            discover.app,
            ["write-scoping", "--project-root", str(repo_root), "--name", "silver.vw_cli_test",
             "--scoping-file", str(scoping_file)],
        )
        assert result.exit_code == 0, result.output
        written = _json.loads(cat_file.read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "analyzed"
    finally:
        cat_file.unlink(missing_ok=True)
        scoping_file.unlink(missing_ok=True)

def test_write_scoping_cli_reports_schema_validation_errors(caplog: pytest.LogCaptureFixture) -> None:
    """write-scoping CLI surfaces schema validation detail for model retry loops."""
    import json as _json
    from typer.testing import CliRunner

    repo_root = Path(__file__).resolve().parents[3]
    tables_dir = repo_root / "catalog" / "tables"
    procs_dir = repo_root / "catalog" / "procedures"
    cat_file = tables_dir / "dbo.t.json"
    proc_file = procs_dir / "dbo.usp_load.json"
    scoping_file = repo_root / ".staging-test-bad-scoping.json"

    try:
        tables_dir.mkdir(parents=True, exist_ok=True)
        procs_dir.mkdir(parents=True, exist_ok=True)
        cat_file.write_text(
            _json.dumps({
                "schema": "dbo",
                "name": "t",
                "columns": [],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        proc_file.write_text(
            _json.dumps({
                "schema": "dbo",
                "name": "usp_load",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "mode": "deterministic",
                "routing_reasons": [],
            }),
            encoding="utf-8",
        )
        scoping_file.write_text(
            _json.dumps({
                "selected_writer": "dbo.usp_load",
                "selected_writer_rationale": "Only writer.",
                "candidates": [{"procedure": "dbo.usp_load"}],
            }),
            encoding="utf-8",
        )

        runner = CliRunner()
        result = runner.invoke(
            discover.app,
            [
                "write-scoping",
                "--project-root",
                str(repo_root),
                "--name",
                "dbo.T",
                "--scoping-file",
                str(scoping_file),
            ],
        )
        assert result.exit_code == 1
        assert "validation errors for TableScopingSection" in caplog.text
        assert "procedure_name" in caplog.text
    finally:
        cat_file.unlink(missing_ok=True)
        proc_file.unlink(missing_ok=True)
        scoping_file.unlink(missing_ok=True)

def test_run_write_scoping_error_diagnostic_forces_error_status() -> None:
    """Error-severity diagnostics on scoping force table status=error."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        tables_dir = root / "catalog" / "tables"
        procs_dir = root / "catalog" / "procedures"
        tables_dir.mkdir(parents=True)
        procs_dir.mkdir(parents=True)

        (tables_dir / "silver.linkedserverexectarget.json").write_text(
            json.dumps({
                "schema": "silver",
                "name": "linkedserverexectarget",
                "columns": [],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )

        scoping = {
            "selected_writer_rationale": "Only candidate delegates through remote EXEC and is unsupported.",
            "candidates": [
                {
                    "procedure_name": "silver.usp_scope_linkedserverexec",
                    "rationale": "Delegates to external procedure through EXEC.",
                }
            ],
            "warnings": [],
            "errors": [
                {
                    "code": "REMOTE_EXEC_UNSUPPORTED",
                    "message": "Writer delegates through linked-server or cross-database EXEC, which is out of scope.",
                    "severity": "error",
                }
            ],
        }

        result = discover.run_write_scoping(root, "silver.LinkedServerExecTarget", scoping)
        assert result["status"] == "ok"
        written = json.loads(Path(result["written"]).read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "error"
        assert written["scoping"]["errors"][0]["code"] == "REMOTE_EXEC_UNSUPPORTED"

def test_run_write_view_scoping_parse_error() -> None:
    """write-view-scoping with DDL_PARSE_ERROR and no sql_elements sets status=error."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_broken.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_broken",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping = {
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": "unexpected token"}],
        }
        result = discover.run_write_view_scoping(root, "silver.vw_broken", scoping)
        assert result["status"] == "ok"
        written = json.loads(Path(result["written"]).read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "error"
        assert written["scoping"]["errors"][0]["code"] == "DDL_PARSE_ERROR"
