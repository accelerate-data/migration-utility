from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.loader_io import write_manifest_sandbox
from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
)
from tests.unit.test_harness.helpers import _cli_env, _write_fixture_manifest


class TestResolveSandboxDb:
    def test_reads_database_from_manifest(self, tmp_path: Path) -> None:
        from shared.test_harness import _resolve_sandbox_db

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000A")

        sandbox_db, manifest = _resolve_sandbox_db(tmp_path)
        assert sandbox_db == "SBX_00000000000A"
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

class TestCLISandboxUpPersists:
    """E2E: invoke sandbox-up via CliRunner and verify manifest.json is updated."""

    def test_sandbox_up_writes_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="SBX_00000000000B",
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
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "SBX_00000000000B"

    def test_sandbox_up_error_does_not_write_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="SBX_00000000000B",
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
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "SBX_TEMPLATE0000"

class TestCLISandboxDownClears:
    """E2E: invoke sandbox-down via CliRunner and verify manifest.json is cleared."""

    def test_sandbox_down_clears_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000B")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="SBX_00000000000B", status="ok",
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
        write_manifest_sandbox(tmp_path, "SBX_00000000000A")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="SBX_00000000000A", status="ok",
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_down.assert_called_once_with(sandbox_db="SBX_00000000000A")

    def test_sandbox_down_error_exits_1_and_preserves_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000A")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="SBX_00000000000A",
            status="error",
            errors=[ErrorEntry(code="SANDBOX_DROP_FAILED", message="drop failed")],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        backend_mock.sandbox_down.assert_called_once_with(sandbox_db="SBX_00000000000A")
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "SBX_00000000000A"

class TestCLIStatusFallback:
    """E2E: invoke sandbox-status, verify manifest-based sandbox_db resolution."""

    def test_sandbox_status_uses_manifest_sandbox_db(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_00000000000A")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_status.return_value = SandboxStatusOutput(
            sandbox_database="SBX_00000000000A", status="ok", exists=True,
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_status.assert_called_once_with(
            sandbox_db="SBX_00000000000A",
            schemas=["dbo", "silver"],
        )

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

    def test_sandbox_status_mentions_restart_when_password_env_exists_in_dotenv(self, tmp_path: Path) -> None:
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
                            "connection": {
                                "host": "127.0.0.1",
                                "port": "1433",
                                "database": "KimballFixture",
                                "user": "sa",
                                "password_env": "SOURCE_MSSQL_PASSWORD",
                            },
                        },
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {
                                "host": "127.0.0.1",
                                "port": "1433",
                                "database": "SBX_000000000001",
                                "user": "sa",
                                "password_env": "SANDBOX_MSSQL_PASSWORD",
                            },
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text(
            "SOURCE_MSSQL_PASSWORD=source-pass\nSANDBOX_MSSQL_PASSWORD=sandbox-pass\n",
            encoding="utf-8",
        )

        runner = CliRunner()
        with patch("shared.test_harness.resolve_project_root", return_value=tmp_path), patch.dict(os.environ, {}, clear=True):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        message = payload["errors"][0]["message"]
        assert "SANDBOX_MSSQL_PASSWORD" in message
        assert "defined in .env" in message
        assert "direnv" in message
