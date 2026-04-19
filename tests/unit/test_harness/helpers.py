from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).parent / "fixtures"


def _write_fixture_manifest(dest: Path) -> None:
    """Copy the standard test manifest fixture to dest."""
    shutil.copy(FIXTURES / "manifest.json", dest / "manifest.json")

def _cli_env(tmp_path: Path) -> dict[str, str]:
    """Env vars needed for SqlServerSandbox.from_env in CLI tests."""
    return {
        "MSSQL_HOST": "localhost",
        "MSSQL_PORT": "1433",
        "SA_PASSWORD": "TestPass123",
        "MSSQL_DB": "TestDB",
    }

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
