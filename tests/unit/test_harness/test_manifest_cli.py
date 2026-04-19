from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.loader_io import clear_manifest_sandbox, read_manifest, write_manifest_sandbox
from shared.sandbox.base import SandboxBackend
from tests.unit.test_harness.helpers import FIXTURES, _write_fixture_manifest


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

class TestWriteManifestSandbox:
    def test_persist_sandbox_to_manifest(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_000000000123")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "SBX_000000000123"
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"
        assert manifest["extraction"]["schemas"] == ["dbo", "silver"]

    def test_persist_overwrites_existing_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "SBX_000000000002")
        write_manifest_sandbox(tmp_path, "SBX_000000000003")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "SBX_000000000003"

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
            write_manifest_sandbox(tmp_path, "SBX_000000000123")

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
        write_manifest_sandbox(tmp_path, "SBX_000000000123")
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
