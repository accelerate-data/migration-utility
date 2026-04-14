"""Regression checks for the SQL Server image publisher fixture paths."""

from __future__ import annotations

from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def test_publish_sqlserver_image_uses_demo_warehouse_paths() -> None:
    script_path = REPO_ROOT / "scripts/publish-sqlserver-image.sh"
    script_text = script_path.read_text(encoding="utf-8")

    expected_paths = [
        "scripts/demo-warehouse/schema/sqlserver.sql",
        "scripts/demo-warehouse/data/baseline/sqlserver.sql",
        "scripts/demo-warehouse/procedures/sqlserver.sql",
        "scripts/demo-warehouse/data/delta",
    ]

    for relative_path in expected_paths:
        assert relative_path in script_text
        assert (REPO_ROOT / relative_path).exists()

    assert "test-fixtures/" not in script_text

