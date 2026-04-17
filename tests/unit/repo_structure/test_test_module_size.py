"""Regression checks for oversized Python test module splits."""

from __future__ import annotations

from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()

SPLIT_MODULE_LIMIT = 800
SPLIT_MODULES = [
    "tests/unit/discover/test_discover.py",
    "tests/unit/dry_run/test_dry_run.py",
]


def test_split_test_modules_stay_focused_by_behavior_area() -> None:
    oversized = {
        path: len((REPO_ROOT / path).read_text(encoding="utf-8").splitlines())
        for path in SPLIT_MODULES
        if (REPO_ROOT / path).exists()
    }

    assert oversized == {}

    split_files = [
        path
        for area in ("discover", "dry_run")
        for path in (REPO_ROOT / "tests" / "unit" / area).glob("test_*.py")
    ]

    assert split_files
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= SPLIT_MODULE_LIMIT
        for path in split_files
    )
