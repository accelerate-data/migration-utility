"""CLI commands via CliRunner tests."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from shared.loader_io import clear_manifest_sandbox, read_manifest, write_manifest_sandbox
from shared.output_models.sandbox import (
    ErrorEntry,
    ExecuteSpecOutput,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.sandbox.base import SandboxBackend

FIXTURES = Path(__file__).parent / "fixtures"


# ── CLI manifest routing ─────────────────────────────────────────────────────


class TestCLIManifestRouting:
    def test_load_manifest_returns_technology(self, tmp_path: Path) -> None:
        shutil.copy(FIXTURES / "manifest.json", tmp_path / "manifest.json")
        from shared.test_harness import _load_manifest

        manifest = _load_manifest(tmp_path)
        assert manifest["technology"] == "sql_server"
        assert manifest["runtime"]["source"]["connection"]["database"] == "TestDB"
        assert manifest["extraction"]["schemas"] == ["dbo", "silver"]

    def test_load_manifest_missing_raises(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _load_manifest

        with pytest.raises(Exit):
            _load_manifest(tmp_path)

    def test_load_manifest_accepts_runtime_only_technology(self, tmp_path: Path) -> None:
        from shared.test_harness import _load_manifest

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "runtime": {
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SANDBOXPDB"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        manifest = _load_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["technology"] == "oracle"

    def test_create_backend_prefers_runtime_sandbox_technology(self) -> None:
        from shared.test_harness_support.manifest import _create_backend

        backend_cls = MagicMock()
        backend_instance = MagicMock(spec=SandboxBackend)
        backend_cls.from_env.return_value = backend_instance
        with patch("shared.test_harness_support.manifest.get_backend", return_value=backend_cls) as mock_get_backend:
            backend = _create_backend(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "MigrationTest"},
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SANDBOXPDB"},
                        },
                    },
                }
            )

        mock_get_backend.assert_called_once_with("oracle")
        assert backend is backend_instance


# ── Manifest sandbox persistence ──────────────────────────────────────────────


def _write_fixture_manifest(dest: Path) -> None:
    """Copy the standard test manifest fixture to dest."""
    shutil.copy(FIXTURES / "manifest.json", dest / "manifest.json")


class TestWriteManifestSandbox:
    def test_persist_sandbox_to_manifest(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_run_123")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_run_123"
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"
        assert manifest["extraction"]["schemas"] == ["dbo", "silver"]

    def test_persist_overwrites_existing_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_old_run")
        write_manifest_sandbox(tmp_path, "__test_new_run")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_new_run"

    def test_missing_runtime_sandbox_raises(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="runtime.sandbox"):
            write_manifest_sandbox(tmp_path, "__test_run_123")

    def test_preserves_existing_oracle_sandbox_role(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SRCPDB", "schema": "SH"},
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SANDBOXPDB", "schema": "TEMPLATE"},
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        write_manifest_sandbox(tmp_path, "SANDBOX_USER")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["technology"] == "oracle"
        assert manifest["runtime"]["sandbox"]["connection"]["schema"] == "SANDBOX_USER"


class TestClearManifestSandbox:
    def test_clear_removes_sandbox_key(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_run_123")
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest.get("runtime", {})
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"

    def test_clear_noop_when_no_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest.get("runtime", {})


class TestResolveSandboxDb:
    def test_reads_database_from_manifest(self, tmp_path: Path) -> None:
        from shared.test_harness import _resolve_sandbox_db

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")

        sandbox_db, manifest = _resolve_sandbox_db(tmp_path)
        assert sandbox_db == "__test_manifest_run"
        assert "technology" in manifest

    def test_missing_sandbox_exits(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _resolve_sandbox_db

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                    "extraction": {
                        "schemas": ["dbo", "silver"],
                        "extracted_at": "2026-03-31T00:00:00Z",
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(Exit):
            _resolve_sandbox_db(tmp_path)

    def test_manifest_read_error_uses_strict_loader(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        from click.exceptions import Exit

        from shared.test_harness_support import manifest as manifest_helpers

        with patch.object(manifest_helpers, "read_manifest", side_effect=PermissionError("permission denied")):
            with pytest.raises(Exit) as exc_info:
                manifest_helpers._resolve_sandbox_db(tmp_path)

        assert exc_info.value.exit_code == 2
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "error"
        assert output["errors"][0]["code"] == "MANIFEST_READ_ERROR"


# ── E2E CLI invocation ────────────────────────────────────────────────────────


def _cli_env(tmp_path: Path) -> dict[str, str]:
    """Env vars needed for SqlServerSandbox.from_env in CLI tests."""
    return {
        "MSSQL_HOST": "localhost",
        "MSSQL_PORT": "1433",
        "SA_PASSWORD": "TestPass123",
        "MSSQL_DB": "TestDB",
    }


class TestCLISandboxUpPersists:
    """E2E: invoke sandbox-up via CliRunner and verify manifest.json is updated."""

    def test_sandbox_up_writes_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="__test_e2e_run",
            status="ok",
            tables_cloned=["dbo.Product"],
            views_cloned=[],
            procedures_cloned=[],
            errors=[],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_e2e_run"

    def test_sandbox_up_error_does_not_write_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="__test_e2e_run",
            status="error",
            tables_cloned=[],
            views_cloned=[],
            procedures_cloned=[],
            errors=[ErrorEntry(code="CONNECT_FAILED", message="timeout")],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_template"


class TestCLISandboxDownClears:
    """E2E: invoke sandbox-down via CliRunner and verify manifest.json is cleared."""

    def test_sandbox_down_clears_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="__test_e2e_run", status="ok",
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "sandbox" not in manifest.get("runtime", {})

    def test_sandbox_down_reads_sandbox_db_from_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="__test_manifest_run", status="ok",
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_down.assert_called_once_with(sandbox_db="__test_manifest_run")


class TestCLIStatusFallback:
    """E2E: invoke sandbox-status, verify manifest-based sandbox_db resolution."""

    def test_sandbox_status_uses_manifest_sandbox_db(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_status.return_value = SandboxStatusOutput(
            sandbox_database="__test_manifest_run", status="ok", exists=True,
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_status.assert_called_once_with(sandbox_db="__test_manifest_run")

    def test_sandbox_status_no_sandbox_in_manifest_exits(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                    "extraction": {
                        "schemas": ["dbo", "silver"],
                        "extracted_at": "2026-03-31T00:00:00Z",
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["errors"][0]["code"] == "SANDBOX_NOT_CONFIGURED"


# ── execute-spec CLI ─────────────────────────────────────────────────────────


def _write_test_spec(path: Path, unit_tests: list[dict[str, Any]]) -> Path:
    """Write a minimal test spec JSON file and return its path."""
    spec = {
        "item_id": "silver.dimproduct",
        "status": "ok",
        "coverage": "complete",
        "branch_manifest": [],
        "unit_tests": unit_tests,
        "uncovered_branches": [],
        "warnings": [],
        "validation": {"passed": True, "issues": []},
        "errors": [],
    }
    spec_path = path / "test-specs" / "silver.dimproduct.json"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(json.dumps(spec, indent=2))
    return spec_path


class TestCLIExecuteSpec:
    """E2E: invoke execute-spec via CliRunner, verify expect.rows written back."""

    def test_execute_spec_writes_expect_rows(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_merge_matched",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load_DimProduct]",
                "given": [
                    {"table": "[bronze].[Product]", "rows": [{"id": 1}]},
                ],
            }
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_merge_matched",
            status="ok",
            ground_truth_rows=[{"ProductKey": 1, "Name": "Widget"}],
            row_count=1,
            errors=[],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 0

        # Verify spec file was updated with expect.rows
        updated_spec = json.loads(spec_path.read_text())
        assert updated_spec["unit_tests"][0]["expect"] == {
            "rows": [{"ProductKey": 1, "Name": "Widget"}],
        }

    def test_execute_spec_partial_failure(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_ok",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 2}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.side_effect = [
            TestHarnessExecuteOutput(
                scenario_name="test_ok",
                status="ok",
                ground_truth_rows=[{"id": 1}],
                row_count=1,
                errors=[],
            ),
            TestHarnessExecuteOutput(
                scenario_name="test_fail",
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="SCENARIO_FAILED", message="insert failed")],
            ),
        ]

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 1

    def test_execute_spec_all_fail_exits_1(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_fail",
            status="error",
            ground_truth_rows=[],
            row_count=0,
            errors=[ErrorEntry(code="SCENARIO_FAILED", message="connection refused")],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 1

    def test_execute_spec_output_model(self) -> None:
        result = ExecuteSpecOutput.model_validate({
            "schema_version": "1.0",
            "sandbox_database": "__test_abc123",
            "spec_path": "test-specs/silver.dimproduct.json",
            "total": 2,
            "ok": 1,
            "failed": 1,
            "results": [
                {
                    "scenario_name": "test_merge_matched",
                    "status": "ok",
                    "row_count": 1,
                    "errors": [],
                },
                {
                    "scenario_name": "test_merge_not_matched",
                    "status": "error",
                    "row_count": 0,
                    "errors": [{"code": "SCENARIO_FAILED", "message": "insert failed"}],
                },
            ],
        })
        assert result.ok == 1
        assert result.failed == 1
        assert len(result.results) == 2


# ── Corrupt JSON tests ──────────────────────────────────────────────


class TestCorruptJsonHandling:
    """Verify CLI commands handle corrupt JSON inputs gracefully."""

    def test_sandbox_up_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-up with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_sandbox_status_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-status with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        # sandbox-status reads runtime.sandbox from manifest, which will fail
        result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_execute_spec_corrupt_json_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with corrupt test-spec JSON exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "corrupt-spec.json"
        spec.write_text("{not valid json", encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "__test_abc123"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_execute_spec_missing_required_fields_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with valid JSON but missing unit_tests exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "empty-spec.json"
        spec.write_text('{"model": "stg_test"}', encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "__test_abc123"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1
