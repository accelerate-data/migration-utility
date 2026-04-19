"""Oracle sandbox clone boundary tests."""

from __future__ import annotations

import subprocess
import sys

from shared.sandbox.oracle import OracleSandbox


class TestOracleCloneModuleBoundary:
    def test_extracted_modules_import_without_facade_preload(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import shared.sandbox.oracle_clone; "
                "import shared.sandbox.oracle_lifecycle_core",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr

    def test_clone_methods_live_in_clone_module(self) -> None:
        assert (
            OracleSandbox._load_object_columns.__module__
            == "shared.sandbox.oracle_clone"
        )
        assert (
            OracleSandbox._render_column_type.__module__
            == "shared.sandbox.oracle_clone"
        )
        assert (
            OracleSandbox._create_empty_table.__module__
            == "shared.sandbox.oracle_clone"
        )
        assert (
            OracleSandbox._clone_tables.__module__
            == "shared.sandbox.oracle_clone"
        )
        assert (
            OracleSandbox._clone_views.__module__
            == "shared.sandbox.oracle_clone"
        )
        assert (
            OracleSandbox._clone_procedures.__module__
            == "shared.sandbox.oracle_clone"
        )

    def test_lifecycle_core_methods_live_in_lifecycle_core_module(self) -> None:
        assert (
            OracleSandbox._create_sandbox_pdb.__module__
            == "shared.sandbox.oracle_lifecycle_core"
        )
        assert (
            OracleSandbox._drop_sandbox_pdb.__module__
            == "shared.sandbox.oracle_lifecycle_core"
        )
        assert (
            OracleSandbox._create_sandbox_schema.__module__
            == "shared.sandbox.oracle_lifecycle_core"
        )
