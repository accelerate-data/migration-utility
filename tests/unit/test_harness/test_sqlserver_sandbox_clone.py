"""SQL Server sandbox clone boundary tests."""

from __future__ import annotations

import subprocess
import sys

from shared.sandbox.sql_server import SqlServerSandbox


class TestSqlServerCloneModuleBoundary:
    def test_extracted_modules_import_without_facade_preload(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import shared.sandbox.sql_server_clone; "
                "import shared.sandbox.sql_server_lifecycle_core",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr

    def test_clone_methods_live_in_clone_module(self) -> None:
        assert (
            SqlServerSandbox._load_object_columns.__module__
            == "shared.sandbox.sql_server_clone"
        )
        assert (
            SqlServerSandbox._render_column_type.__module__
            == "shared.sandbox.sql_server_clone"
        )
        assert (
            SqlServerSandbox._create_empty_table.__module__
            == "shared.sandbox.sql_server_clone"
        )
        assert (
            SqlServerSandbox._clone_tables.__module__
            == "shared.sandbox.sql_server_clone"
        )
        assert (
            SqlServerSandbox._clone_views.__module__
            == "shared.sandbox.sql_server_clone"
        )
        assert (
            SqlServerSandbox._clone_procedures.__module__
            == "shared.sandbox.sql_server_clone"
        )

    def test_lifecycle_core_methods_live_in_lifecycle_core_module(self) -> None:
        assert (
            SqlServerSandbox._create_sandbox_db.__module__
            == "shared.sandbox.sql_server_lifecycle_core"
        )
        assert (
            SqlServerSandbox._create_schemas.__module__
            == "shared.sandbox.sql_server_lifecycle_core"
        )
